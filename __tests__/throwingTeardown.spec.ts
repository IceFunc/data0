/**
 * 父 effect 重算时,上一轮子 effect 的清理抛错:prepareTracking 要么完成,要么不留痕迹。
 *
 * prepareTracking 先压栈、再销毁上一轮的子 effect;销毁会执行用户清理。调用点(callSimpleGetter/
 * callAsyncGetter)把它放在 try 之外,基类 run() 又把它放在 try 之内却无条件 resetTracking:
 *   - Computed 宿主永久留在 activeScopes 上,之后任何顶层读取都订阅它(全局污染);
 *   - 裸 effect 宿主丢掉全部依赖,从此不再响应;嵌套时多弹一层 trackStack,dev 断言盖住原始错误;
 *   - async getter 卡在 RECOMPUTING,asyncStatus 恒 true,cleanPromise 永不结束(generator 是对的)。
 *
 * 等价类:{宿主形态} × {顶层/嵌套在别的 effect 里}。每一格都断言:错误按该宿主的通道送达、
 * 全局状态复原、受害者(兄弟订阅者、外层 effect、无关读取)不受影响、恢复后照常工作。
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {
    atom,
    Atom,
    AtomComputed,
    autorun,
    batch,
    Computed,
    computed,
    destroyComputed,
    Notifier,
    notifier,
    ReactiveEffect,
    RxList,
    RxSet,
    STATUS_CLEAN,
    STATUS_DIRTY,
    TrackOpTypes,
} from '../src/index.js'

const subscribers = (target: object) => [...(notifier.getDepEffects(target) ?? [])]

const trackingState = () => ({
    scopes: ReactiveEffect.activeScopes.length,
    depth: notifier.effectTrackDepth,
    bit: Notifier.trackOpBit,
    trackStack: notifier.trackStack.length,
    shouldTrack: notifier.shouldTrack,
})

async function settle() {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

let errorLog: unknown[][]
beforeEach(() => {
    errorLog = []
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation((...args) => { errorLog.push(args) })
})

afterEach(() => {
    vi.restoreAllMocks()
    const state = trackingState()
    // 先复位再断言,一个失败的用例不拖垮后面的
    ReactiveEffect.activeScopes.length = 0
    notifier.effectTrackDepth = 0
    Notifier.trackOpBit = 1
    notifier.trackStack.length = 0
    notifier.shouldTrack = true
    expect(state).toEqual({scopes: 0, depth: 0, bit: 1, trackStack: 0, shouldTrack: true})
})

// 与 axii 的轻量绑定同形:直接继承 ReactiveEffect
class LightEffect extends ReactiveEffect {
    constructor(fn: () => any) {
        super(fn)
        this.run()
    }
    callGetter() {
        return this.getter!()
    }
    run() {
        if (!this.active) return
        return super.run()
    }
}

// delivery:错误送达的通道。sync = 抛给写入方;console = 没有同步调用方,console.error 上报
type Host = {destroy: () => void, effect?: () => Computed}
type HostCase = {delivery: 'sync' | 'console', create: (dep: Atom<number>, body: () => void) => Host}

const HOSTS: Record<string, HostCase> = {
    'autorun': {delivery: 'sync', create: (dep, body) => ({destroy: autorun(() => { dep(); body() }, true)})},
    'autorun, default scheduling': {delivery: 'console', create: (dep, body) => ({destroy: autorun(() => { dep(); body() })})},
    'computed': {
        delivery: 'sync', create: (dep, body) => {
            const c = computed(() => { body(); return dep() })
            return {destroy: () => destroyComputed(c)}
        },
    },
    'light effect': {
        delivery: 'sync', create: (dep, body) => {
            const effect = new LightEffect(() => { dep(); body() })
            return {destroy: () => effect.destroy()}
        },
    },
    'async getter computed': {
        delivery: 'console', create: (dep, body) => {
            const effect = new AtomComputed(async () => { dep(); body() }, undefined, true)
            return {destroy: () => effect.destroy(), effect: () => effect}
        },
    },
    'generator getter computed': {
        delivery: 'console', create: (dep, body) => {
            const effect = new AtomComputed(function* (): any { dep(); body() }, undefined, true)
            return {destroy: () => effect.destroy(), effect: () => effect}
        },
    },
    'patch computed falling back to a full recompute': {
        delivery: 'sync', create: (dep, body) => {
            const c = computed<number>(function (this: Computed) {
                this.manualTrack(dep, TrackOpTypes.ATOM, 'value')
                body()
                return dep.raw
            }, () => false)
            return {destroy: () => destroyComputed(c)}
        },
    },
    'RxSet computed': {
        delivery: 'sync', create: (dep, body) => {
            const set = new RxSet(() => { body(); return [dep()] })
            return {destroy: () => set.destroy()}
        },
    },
    'RxList.map row': {
        delivery: 'sync', create: (dep, body) => {
            const mapped = new RxList([1]).map(item => { dep(); body(); return item })
            return {destroy: () => mapped.destroy()}
        },
    },
}

describe('a child cleanup that throws while its parent re-runs', () => {
    for (const [name, {delivery, create}] of Object.entries(HOSTS)) {
        function setup() {
            const dep = atom(0)
            const tick = atom(0)
            const control = {throws: false, runs: 0, childRuns: 0}
            const host = create(dep, () => {
                control.runs++
                // 抛错的子 effect 在前,正常的在后:后者必须由恢复后的重算销毁
                autorun(({onCleanup}) => { onCleanup(() => { if (control.throws) throw new Error('child cleanup') }) }, true)
                autorun(() => { tick(); control.childRuns++ }, true)
            })
            return {dep, tick, control, host}
        }
        async function expectRecovers({dep, tick, control, host}: ReturnType<typeof setup>) {
            control.throws = false
            const runs = control.runs
            dep(dep.raw + 1)
            await settle()
            expect(control.runs).toBe(runs + 1)
            const children = control.childRuns
            tick(tick.raw + 1)
            await settle()
            // 只剩这一轮创建的那个子 effect:失败那轮留下的没有泄漏
            expect(control.childRuns).toBe(children + 1)
            host.destroy()
            tick(tick.raw + 1)
            await settle()
            expect(control.childRuns).toBe(children + 1)
        }
        function expectDelivered(thrown: unknown) {
            if (delivery === 'sync') {
                expect((thrown as Error)?.message).toBe('child cleanup')
                expect(errorLog).toEqual([])
            } else {
                expect(thrown).toBe(undefined)
                expect(errorLog.length).toBe(1)
                expect((errorLog[0][1] as Error).message).toBe('child cleanup')
            }
        }

        test(`${name}: top level`, async () => {
            const env = setup()
            await settle()
            const before = trackingState()
            // 兄弟订阅者:同一次派发里照常执行
            let siblingRuns = 0
            const stopSibling = autorun(() => { env.dep(); siblingRuns++ }, true)

            env.control.throws = true
            let thrown: unknown
            try { env.dep(1) } catch (err) { thrown = err }
            expect(trackingState()).toEqual(before)
            await settle()
            expectDelivered(thrown)
            expect(trackingState()).toEqual(before)
            expect(siblingRuns).toBe(2)

            // 顶层读取不属于任何 effect
            const unrelated = atom(0)
            unrelated()
            expect(subscribers(unrelated)).toEqual([])
            // dep 上没有残留的 marker 位:之后同一层级的订阅不会被误判为已追踪
            let lateRuns = 0
            const stopLate = autorun(() => { env.dep(); lateRuns++ }, true)
            expect(subscribers(env.dep).length).toBe(3)

            await expectRecovers(env)
            expect(siblingRuns).toBe(3)
            expect(lateRuns).toBe(2)
            stopSibling()
            stopLate()
        })

        test(`${name}: inside another running effect`, async () => {
            const env = setup()
            await settle()
            const go = atom(0)
            const after = atom(0)
            let outerRuns = 0
            let thrown: unknown
            let states: unknown[] = []
            const stopOuter = autorun(() => {
                outerRuns++
                if (go() === 1 && !states.length) {
                    const before = trackingState()
                    try { env.dep(env.dep.raw + 1) } catch (err) { thrown = err }
                    states = [before, trackingState()]
                }
                after()
            }, true)
            env.control.throws = true
            go(1)
            expect(states[1]).toEqual(states[0])
            await settle()
            expectDelivered(thrown)
            // 抛错之后的读取仍属于外层 effect
            after(1)
            expect(outerRuns).toBe(3)
            stopOuter()
            await expectRecovers(env)
        })
    }

    test('inside batch: the other queued subscribers run and the first error reaches the caller', () => {
        const dep = atom(0)
        let shouldThrow = false
        let siblingRuns = 0
        const stop = autorun(() => {
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }, true)
        const stopSibling = autorun(() => { dep(); siblingRuns++ }, true)
        shouldThrow = true
        expect(() => batch(() => { dep(1) })).toThrow('child cleanup')
        expect(siblingRuns).toBe(2)
        shouldThrow = false
        dep(2)
        expect(siblingRuns).toBe(3)
        stop()
        stopSibling()
    })

    test('deeper than the dep markers (full cleanup mode)', () => {
        const atoms = Array.from({length: 33}, () => atom(0))
        const stops = atoms.slice(0, -1).map((_, i) => autorun(() => {
            const value = atoms[i]()
            if (value) atoms[i + 1](value)
        }, true))
        let shouldThrow = false
        let runs = 0
        const stop = autorun(() => {
            runs++
            atoms[32]()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }, true)
        shouldThrow = true
        expect(() => atoms[0](1)).toThrow('child cleanup')
        shouldThrow = false
        atoms[32](100)
        expect(runs).toBe(2)
        stop()
        stops.forEach(s => s())
    })

    test('full cleanup mode: a read in the throwing cleanup leaves no marker bit behind', () => {
        // patch 型 computed 不用依赖标记。清理在抛错前读了 x：x 的 dep 上不能留下本深度的
        // 标记位，否则之后在同一深度读 x 的 effect 会被判为已订阅，从此收不到 x 的变化。
        const source = atom(0)
        const x = atom(0)
        let shouldThrow = false
        const host = computed<number>(function (this: Computed) {
            this.manualTrack(source, TrackOpTypes.ATOM, 'value')
            autorun(({onCleanup}) => { onCleanup(() => { x(); if (shouldThrow) throw new Error('child cleanup') }) }, true)
            return source.raw
        }, () => false)
        host()
        shouldThrow = true
        expect(() => source(1)).toThrow('child cleanup')
        shouldThrow = false

        let runs = 0
        const stop = autorun(() => { x(); runs++ }, true)
        expect(subscribers(x)).toHaveLength(1)
        x(1)
        expect(runs).toBe(2)
        stop()
        destroyComputed(host)
    })

    test('an async getter settles like a generator getter', async () => {
        for (const getter of [async () => { body() }, function* (): any { body() }]) {
            var dep = atom(0)
            var shouldThrow = false
            const effect = new AtomComputed(getter, undefined, true)
            await settle()
            expect(effect.status.raw).toBe(STATUS_CLEAN)
            shouldThrow = true
            let outcome = 'pending'
            effect.recompute(true)!.then(() => { outcome = 'resolved' }, (err: Error) => { outcome = err.message })
            await settle()
            expect(outcome).toBe('child cleanup')
            expect(effect.asyncStatus!.raw).toBe(false)
            expect(effect.status.raw).toBe(STATUS_DIRTY)
            shouldThrow = false
            dep(1)
            await settle()
            expect(effect.status.raw).toBe(STATUS_CLEAN)
            effect.destroy()
        }
        function body() {
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }
    })

    test('a parent destroyed by the throwing cleanup stays destroyed', () => {
        const hosts: Array<(dep: Atom<number>, body: () => void) => ReactiveEffect> = [
            (dep, body) => new LightEffect(() => { dep(); body() }),
            (dep, body) => {
                const effect = new Computed(() => { dep(); body() }, undefined, true)
                effect.run([], true)
                return effect
            },
        ]
        for (const create of hosts) {
            const dep = atom(0)
            let runs = 0
            const parent: ReactiveEffect = create(dep, () => {
                runs++
                autorun(({onCleanup}) => {
                    onCleanup(() => {
                        parent.destroy()
                        throw new Error('child cleanup')
                    })
                }, true)
            })
            expect(() => dep(1)).toThrow('child cleanup')
            expect(parent.active).toBe(false)
            expect(subscribers(dep)).toEqual([])
            dep(2)
            expect(runs).toBe(1)
        }
    })

    test('a subclass that calls prepareTracking itself', () => {
        const dep = atom(0)
        let shouldThrow = false
        class Manual extends ReactiveEffect {
            runs = 0
            constructor() {
                super(() => {})
                this.refresh()
            }
            run() {
                this.refresh()
            }
            refresh() {
                this.prepareTracking()
                notifier.enableTracking()
                try {
                    this.runs++
                    dep()
                    autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
                } finally {
                    notifier.resetTracking()
                    this.completeTracking()
                }
            }
        }
        const effect = new Manual()
        shouldThrow = true
        expect(() => dep(1)).toThrow('child cleanup')
        shouldThrow = false
        dep(2)
        expect(effect.runs).toBe(2)
        effect.destroy()
    })
})
