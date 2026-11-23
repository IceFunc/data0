import {afterEach, describe, expect, test, vi} from 'vitest'
import {atom, Atom, batch, computed, onChange, RxList, RxSet} from '../src/index'
import {mulberry32} from './fuzzKit'

/**
 * Regression tests for the branches touched by the audit fixes (the minimal
 * reproductions themselves live in audit-list.spec.ts).
 */

const expectIndexAtomsAligned = (list: RxList<any>) => {
    const atoms = list.atomIndexes
    if (!atoms) return
    expect(atoms.length).toBeLessThanOrEqual(list.data.length)
    atoms.forEach((indexAtom, i) => { expect(indexAtom.raw).toBe(i) })
}

describe('map: an operation-time index beyond the final source never reaches source.atomIndexes', () => {
    test('the replay falls back exactly once and the rebuilt rows track their real positions', () => {
        const live = atom(7)
        const source = new RxList<number | 'live'>([1, 2])
        const calls: any[] = []
        const mapped = source.map(x => { calls.push(x); return x === 'live' ? live() : x })
        let fullRecomputes = 0
        mapped.on('fullRecompute', () => fullRecomputes++)
        batch(() => {
            source.push('live')     // operation-time index 2
            source.splice(0, 2)     // final: ['live'] at index 0
        })
        expect(fullRecomputes).toBe(1)
        expect(mapped.data).toEqual([7])
        expectIndexAtomsAligned(source)
        // the surviving row is the rebuilt one: it follows its real position
        source.unshift(1)
        expect(mapped.data).toEqual([1, 7])
        live(8)
        expect(mapped.data).toEqual([1, 8])
        source.swap(0, 1)
        live(9)
        expect(mapped.data).toEqual([9, 1])
        expectIndexAtomsAligned(source)
        mapped.destroy()
        expect(source.atomIndexes).toBeUndefined()
    })
    test('the row built out of range is inert: its dependency never re-runs mapFn with a stale position', () => {
        // deferred map: the patch runs outside any digest, so a write made by a later
        // mapFn call triggers the earlier (out-of-range) row inline, in the middle of the replay
        const pending: (() => void)[] = []
        const flush = () => pending.splice(0).forEach(run => run())
        const dep = atom(0)
        const source = new RxList<string>([])
        const calls: any[] = []
        const mapped = source.map(x => {
            calls.push(x)
            if (x === 'reader') return `reader:${dep()}`
            if (x === 'writer') dep(dep.raw + 1)
            return x
        }, {scheduleRecompute: (run: any) => { pending.push(run) }})
        source.push('reader')       // operation-time index 0
        source.push('writer')       // its mapFn writes `dep` while the replay is still running
        source.splice(0, 2)         // final length 0
        flush()
        expect(calls).toEqual(['reader', 'writer'])     // never called with undefined / a foreign row
        expect(mapped.data).toEqual([])
        expectIndexAtomsAligned(source)
        dep(10)
        expect(calls).toEqual(['reader', 'writer'])     // the dead row does not come back
        expect(() => source.push('x')).not.toThrow()
        flush()
        expect(mapped.data).toEqual(['x'])
    })
    test('the fallback is aborted by a throwing recompute hook: the out-of-range row stays inert until the retry rebuilds it', () => {
        const live = atom(7)
        const source = new RxList<number | 'live'>([1, 2])
        const calls: any[] = []
        const mapped = source.map(x => { calls.push(x); return x === 'live' ? live() : x })
        // 'recompute' is dispatched once for the patch round and once more for the full recompute it falls back to
        let dispatches = 0
        let armed = false
        mapped.on('recompute', () => { if (armed && ++dispatches === 2) { armed = false; throw new Error('hook boom') } })
        armed = true
        expect(() => batch(() => {
            source.push('live')     // operation-time index 2
            source.splice(0, 2)     // final: ['live'] at index 0 -> the row built at index 2 survives the replay
        })).toThrow('hook boom')
        expectIndexAtomsAligned(source)
        expect(mapped.data).toEqual([7])                // the replay itself was applied; only the rebuild is missing
        calls.length = 0
        // the half-replayed row is still alive (the rebuild never ran) but holds no index:
        // its dependency must not write row 2 of a one-row list / call mapFn with source.data[2]
        live(8)
        expect(calls).toEqual([])
        expect(mapped.data).toEqual([7])
        // the next trigger rebuilds from the final source
        source.push(3)
        expect(mapped.data).toEqual([8, 3])
        live(9)
        expect(mapped.data).toEqual([9, 3])
        expectIndexAtomsAligned(source)
    })
    test('index-param map behind a multi-step upstream (slice window) keeps aligned atoms', () => {
        const live = atom(1)
        const source = new RxList<number>([1, 2, 3, 4, 5])
        const windowed = source.slice(1, 3)
        const mapped = windowed.map(x => x % 2 ? x : x * live())
        batch(() => { source.push(6); source.splice(0, 5) })
        expect(mapped.data).toEqual(windowed.data.map(x => x % 2 ? x : x * live.raw))
        expectIndexAtomsAligned(windowed)
        source.unshift(8, 10, 12)
        live(3)
        expect(mapped.data).toEqual(windowed.data.map(x => x % 2 ? x : x * 3))
        expectIndexAtomsAligned(windowed)
    })
})

describe('map options.onCleanup: exactly once per value that leaves the list, on every path', () => {
    type Token = { id: number, item: string }
    const setup = (useContext = false) => {
        const theme = atom('light')
        let nextId = 1
        const created: Token[] = []
        const cleaned: Token[] = []
        const source = new RxList<string>(['a', 'b', 'c'])
        const make = (item: string): Token => { theme(); const token = {id: nextId++, item}; created.push(token); return token }
        const mapped = useContext
            ? source.map((item, _index, _ctx) => make(item), {onCleanup: (token: Token) => cleaned.push(token)})
            : source.map(item => make(item), {onCleanup: (token: Token) => cleaned.push(token)})
        const check = () => {
            // no double cleanup
            expect(new Set(cleaned).size).toBe(cleaned.length)
            // never for a value still in the list
            for (const token of mapped.data) expect(cleaned.includes(token)).toBe(false)
            // everything that left the list was cleaned
            const inList = new Set(mapped.data)
            expect(created.filter(token => !inList.has(token) && !cleaned.includes(token))).toEqual([])
        }
        return {theme, source, mapped, created, cleaned, check}
    }
    for (const useContext of [false, true]) {
        test(`splice / set / row recompute / reorder / batch / full rebuild / destroy (context=${useContext})`, () => {
            const {theme, source, mapped, created, cleaned, check} = setup(useContext)
            check()
            source.splice(1, 1); check()                         // splice
            source.set(0, 'A'); check()                          // set
            theme('dark'); check()                               // row recompute (every row)
            expect(cleaned.length).toBe(1 + 1 + 2)
            source.swap(0, 1); theme('blue'); check()            // reorder, then row recompute at the new positions
            batch(() => { theme('red'); source.unshift('z'); source.pop() }); check()   // row recompute while structural infos are pending
            batch(() => { source.push('y'); theme('green') }); check()
            mapped.recompute(true); check()                      // full rebuild
            source.clear(); check()
            source.push('k', 'l'); theme('last'); check()
            mapped.destroy()
            expect(cleaned.length).toBe(created.length)          // destroy: the rest, once each
            expect(new Set(cleaned).size).toBe(created.length)
        })
    }
    test('a row that recomputes to the very same value is not cleaned while it is still in the list', () => {
        const tick = atom(0)
        const widgets = new Map<string, object>()
        const widgetOf = (item: string) => { if (!widgets.has(item)) widgets.set(item, {item}); return widgets.get(item)! }
        const cleaned: object[] = []
        const source = new RxList<string>(['a', 'b'])
        const mapped = source.map(item => { tick(); return widgetOf(item) }, {onCleanup: widget => cleaned.push(widget)})
        tick(1); tick(2)
        expect(mapped.data).toEqual([widgetOf('a'), widgetOf('b')])
        expect(cleaned).toEqual([])
        source.shift()
        expect(cleaned).toEqual([widgetOf('a')])
        mapped.destroy()
        expect(cleaned).toEqual([widgetOf('a'), widgetOf('b')])
    })
    test('the callback runs isolated: what it reads does not become a dependency of the row', () => {
        const theme = atom('light')
        const audit = atom(0)
        let runs = 0
        const source = new RxList<number>([1])
        const mapped = source.map(item => { runs++; return `${item}-${theme()}` }, {onCleanup: () => { audit() }})
        theme('dark')
        expect(runs).toBe(2)
        audit(1); audit(2)
        expect(runs).toBe(2)
        expect(mapped.data).toEqual(['1-dark'])
        mapped.destroy()
    })
})

describe('sortSelf: undefined elements follow Array#sort (never passed to the comparator, always last)', () => {
    test('differential against Array#sort: random lists with undefined and duplicates, comparator never sees undefined', () => {
        const rand = mulberry32(20261123)
        for (let round = 0; round < 300; round++) {
            const length = Math.floor(rand() * 9)
            const data = Array.from({length}, () => rand() < 0.3 ? undefined : Math.floor(rand() * 5))
            const list = new RxList<number | undefined>(data.slice())
            let sawUndefined = false
            const cmp = (a: number | undefined, b: number | undefined) => {
                if (a === undefined || b === undefined) sawUndefined = true
                return (a as number) - (b as number)
            }
            list.sortSelf(cmp)
            expect(sawUndefined, `round ${round} [${data}]`).toBe(false)
            expect(list.data, `round ${round} [${data}]`).toEqual(data.slice().sort((a, b) => (a as number) - (b as number)))
            list.destroy()
        }
    })
    test('stable for ties, and a list without undefined sorts exactly as before', () => {
        const rows = [{k: 2, id: 'a'}, {k: 1, id: 'b'}, {k: 2, id: 'c'}, {k: 1, id: 'd'}, {k: 0, id: 'e'}]
        const list = new RxList(rows.slice())
        list.sortSelf((x, y) => x.k - y.k)
        expect(list.data.map(r => r.id)).toEqual(rows.slice().sort((x, y) => x.k - y.k).map(r => r.id))
    })
    test('holes sort after explicit undefined (same value order as Array#sort); reorder pairs stay a full permutation', () => {
        // eslint-disable-next-line no-sparse-arrays
        const data: (number | undefined)[] = [3, , 1, undefined, 2]
        const native = data.slice().sort((a, b) => (a as number) - (b as number))
        const list = new RxList<number | undefined>(data.slice())
        const orders: [number, number][][] = []
        const stop = onChange(list, (infos: any[]) => infos.forEach(info => { if (info.method === 'reorder') orders.push(info.argv[0]) }))
        list.sortSelf((a, b) => (a as number) - (b as number))
        expect(Array.from(list.data)).toEqual(Array.from(native))            // [1, 2, 3, undefined, undefined(hole)]
        expect(orders.length).toBe(1)
        const order = orders[0]
        expect(order.map(pair => pair[1])).toEqual([0, 1, 2, 3, 4])
        expect(order.map(pair => pair[0]).slice().sort()).toEqual([0, 1, 2, 3, 4])
        expect(order.find(pair => pair[0] === 3)![1]).toBe(3)                 // the explicit undefined
        expect(order.find(pair => pair[0] === 1)![1]).toBe(4)                 // the hole goes last
        stop()
    })
    test('derived structures follow the emitted reorder (undefined rows included)', () => {
        const rand = mulberry32(7)
        for (let round = 0; round < 60; round++) {
            const data = Array.from({length: 2 + Math.floor(rand() * 7)}, () => rand() < 0.3 ? undefined : Math.floor(rand() * 6))
            const source = new RxList<number | undefined>(data.slice())
            const selected = new RxSet<number | undefined>([undefined, 2])
            const cmp = (a: number | undefined, b: number | undefined) => (b as number) - (a as number)
            const total = (a: number | undefined, b: number | undefined) => a === undefined ? (b === undefined ? 0 : 1) : b === undefined ? -1 : a - b
            const mapped = source.map((x, i) => `${x}@${i()}`)
            const filtered = source.filter(x => x === undefined || x % 2 === 0)
            const sorted = source.toSorted(total)
            const sliced = source.slice(1, 4)
            const grouped = source.groupBy(x => x === undefined ? 'u' : x % 2)
            const selection = source.createSelection(selected as any)
            const indexSelection = source.createIndexKeySelection(atom<number | null>(1))
            const firstUndefined = source.findIndex(x => x === undefined)
            const last = computed(() => source.at(-1))
            const second = computed(() => source.at(1))
            source.sortSelf(cmp)
            const S = source.data
            const ctx = `round ${round} [${data}]`
            expect(S, ctx).toEqual(data.slice().sort(cmp))
            expect(mapped.data, ctx).toEqual(S.map((x, i) => `${x}@${i}`))
            expect(filtered.data, ctx).toEqual(S.filter(x => x === undefined || x % 2 === 0))
            expect(sorted.data, ctx).toEqual(S.slice().sort(total))
            expect(sliced.data, ctx).toEqual(S.slice(1, 4))
            for (const key of ['u', 0, 1] as const) {
                expect(grouped.data.get(key)?.data ?? [], `${ctx} group ${key}`).toEqual(S.filter(x => (x === undefined ? 'u' : x % 2) === key))
            }
            expect(selection.data.map(row => [row[0], row[1].raw]), ctx).toEqual(S.map(x => [x, x === undefined || x === 2]))
            expect(indexSelection.data.map(row => [row[0], row[1].raw]), ctx).toEqual(S.map((x, i) => [x, i === 1]))
            expect(firstUndefined.raw, ctx).toBe(S.findIndex(x => x === undefined))
            expect(last.raw, ctx).toBe(S.at(-1))
            expect(second.raw, ctx).toBe(S.at(1))
            expect(source.atomIndexes!.map(indexAtom => indexAtom.raw), ctx).toEqual(S.map((_, i) => i))
        }
    })
    test('characterization: a comparator-less call (outside the TS signature) still fails before anything is moved', () => {
        const list = new RxList([3, 1, 2])
        expect(() => (list as any).sortSelf()).toThrow(TypeError)
        expect(list.data).toEqual([3, 1, 2])
    })
})

const time = (fn: () => void) => { const t0 = performance.now(); fn(); return performance.now() - t0 }
const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]

describe('filter: tail operations cost O(suffix); the mid-list path is unchanged', () => {
    const build = (n: number) => {
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const filtered = source.filter(x => x % 2 === 0)
        return {source, filtered}
    }
    const measure = (n: number, op: (source: RxList<number>, i: number) => void, repeat: number) => median([0, 1, 2].map(() => {
        const {source, filtered} = build(n)
        const t = time(() => { for (let i = 0; i < repeat; i++) op(source, i) })
        expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
        filtered.destroy(); source.destroy()
        return t
    }))
    test('timings at 5k and 80k rows: push / pop / tail replace are flat, mid-list splice stays linear', () => {
        measure(1000, (s, i) => s.push(i), 200) // warm up
        const rows: string[] = []
        const cases: [string, (s: RxList<number>, i: number) => void, number][] = [
            ['push x2000', (s, i) => s.push(i), 2000],
            ['pop x2000', s => s.pop(), 2000],
            ['splice(len-2, 2, a, b) x2000', (s, i) => s.splice(s.data.length - 2, 2, i, i + 1), 2000],
            ['mid-list splice(len/2, 1, a) x300', (s, i) => s.splice(s.data.length >> 1, 1, i), 300],
            ['mid-list set(len/2, a) x300', (s, i) => s.set(s.data.length >> 1, i), 300],
        ]
        const ratios: Record<string, number> = {}
        for (const [name, op, repeat] of cases) {
            const small = measure(5000, op, repeat)
            const large = measure(80000, op, repeat)
            ratios[name] = large / small
            rows.push(`${name}: 5k ${small.toFixed(1)}ms, 80k ${large.toFixed(1)}ms, ratio ${(large / small).toFixed(1)}`)
        }
        console.log('FILTER TIMINGS\n  ' + rows.join('\n  '))
        expect(ratios['push x2000']).toBeLessThan(4)
        expect(ratios['pop x2000']).toBeLessThan(4)
        expect(ratios['splice(len-2, 2, a, b) x2000']).toBeLessThan(4)
    }, 120000)
    test('tail shapes: truncation, tail replace with mixed matches, clear, replace-all, batched tail ops, reactive predicate', () => {
        const flip = atom(0)
        const source = new RxList<number>([1, 2, 3, 4, 5, 6, 7, 8])
        const filtered = source.filter(x => (x + flip()) % 2 === 0)
        const mirror = filtered.map(x => x)                       // a downstream consumer of the emitted splices
        const check = () => {
            const model = source.data.filter(x => (x + flip.raw) % 2 === 0)
            expect(filtered.data).toEqual(model)
            expect(mirror.data).toEqual(model)
        }
        check()
        source.splice(5, Infinity); check()                        // truncate: deletes matched and unmatched rows
        source.splice(3, 2, 10, 11, 12); check()                   // tail replace, inserts mixed
        source.push(13, 14, 15, 16); check()
        source.pop(); check()
        batch(() => { source.push(20); source.push(21); source.pop(); source.push(22, 24) }); check()
        batch(() => { source.pop(); flip(1); source.push(30, 31) }); check()   // predicate dep changes in the same batch
        batch(() => { source.unshift(40); source.push(41); source.splice(source.data.length - 1, 1, 42, 43) }); check()
        source.splice(0, source.data.length, 50, 51, 52); check() // replace-all (range reaches the end from 0)
        source.clear(); check()
        source.push(60); source.push(61); check()
        source.splice(1, 0); check()                               // no-op tail splice
        source.splice(99, 5, 62); check()                          // out-of-range start clamps to the tail
        source.splice(-1, 1); check()                              // negative start
    })
    test('mid-list path: differential against Array#filter for random interior splices and sets', () => {
        const rand = mulberry32(42)
        const source = new RxList<number>(Array.from({length: 12}, (_, i) => i))
        const filtered = source.filter(x => x % 3 !== 0)
        for (let step = 0; step < 400; step++) {
            const len = source.data.length
            if (len < 6) { source.push(step, step + 1, step + 2); continue }
            const start = 1 + Math.floor(rand() * (len - 3))
            if (rand() < 0.3) source.set(start, Math.floor(rand() * 50))
            else source.splice(start, Math.floor(rand() * Math.min(3, len - start - 1)), ...Array.from({length: Math.floor(rand() * 3)}, () => Math.floor(rand() * 50)))
            expect(filtered.data, `step ${step}`).toEqual(source.data.filter(x => x % 3 !== 0))
        }
    })
})

describe('spliceArray: items may be the list\'s own data array', () => {
    for (const size of [3, 9000]) {   // 9000 > the native-splice spread limit: exercises the manual-move path too
        test(`self append / prepend / interior insert / replace-with-self (size ${size})`, () => {
            const base = Array.from({length: size}, (_, i) => i)
            const run = (apply: (list: RxList<number>) => void, model: (arr: number[]) => number[]) => {
                const list = new RxList<number>(base.slice())
                const mapped = list.map(x => x * 10)
                const indexed = list.map((x, i) => x - i())
                const evens = list.filter(x => x % 2 === 0)
                const len = list.length
                apply(list)
                const expected = model(base.slice())
                expect(list.data).toEqual(expected)
                expect(mapped.data).toEqual(expected.map(x => x * 10))
                expect(indexed.data).toEqual(expected.map((x, i) => x - i))
                expect(evens.data).toEqual(expected.filter(x => x % 2 === 0))
                expect(len.raw).toBe(expected.length)
            }
            run(list => list.spliceArray(list.data.length, 0, list.data), arr => arr.concat(arr))
            run(list => list.spliceArray(0, 0, list.data), arr => arr.concat(arr))
            run(list => list.spliceArray(1, 1, list.data), arr => [arr[0], ...arr, ...arr.slice(2)])
            run(list => list.spliceArray(0, list.data.length, list.data), arr => arr)
            run(list => list.replaceData(list.data), arr => arr)
        })
    }
    test('the returned deleted items and the payload are independent of the list afterwards', () => {
        const list = new RxList<number>([1, 2, 3])
        const seen: any[][] = []
        const stop = onChange(list, (infos: any[]) => infos.forEach(info => seen.push(info.argv.slice(2))))
        const removed = list.spliceArray(1, 2, list.data)
        expect(removed).toEqual([2, 3])
        expect(list.data).toEqual([1, 1, 2, 3])
        expect(seen).toEqual([[1, 2, 3]])
        stop()
    })
})

describe('at(): non-integer indexes follow Array#at (ToIntegerOrInfinity)', () => {
    const args = [1.5, 1.999, 0.9, -0.9, -0.5, -1.5, -2.2, -3.999, 2.5, 3.5, -4.5, NaN, Infinity, -Infinity, -0, 0, 1, 2, 3, -1, -3, -4, 100.5, -100.5]
    test('same result as Array#at for every argument', () => {
        for (const data of [[], [10], [10, 20, 30]]) {
            const list = new RxList<number>(data.slice())
            for (const arg of args) {
                expect(list.at(arg), `[${data}].at(${arg})`).toBe(data.at(arg))
            }
        }
    })
    test('a fractional index subscribes to the row it reads', () => {
        const list = new RxList<number>([10, 20, 30])
        const second = computed(() => list.at(1.7))
        const lastish = computed(() => list.at(-1.5))
        const first = computed(() => list.at(NaN))
        expect([second.raw, lastish.raw, first.raw]).toEqual([20, 30, 10])
        list.set(1, 21)
        expect(second.raw).toBe(21)
        list.push(40)
        expect(lastish.raw).toBe(40)
        list.unshift(5)
        expect([second.raw, lastish.raw, first.raw]).toEqual([10, 40, 5])
        list.swap(0, 1)
        expect([second.raw, first.raw]).toEqual([5, 10])
        list.splice(0, Infinity)
        expect([second.raw, lastish.raw, first.raw]).toEqual([undefined, undefined, undefined])
    })
})

describe('map: a row cleanup that throws does not keep the other rows from being cleaned', () => {
    afterEach(() => { vi.restoreAllMocks() })
    // rows 1..4; the context cleanup of rows 1 and 3 throws, options.onCleanup throws for the value of row 2
    const setup = () => {
        const dep = atom(0)
        const source = new RxList<number>([1, 2, 3, 4])
        const contextCleaned: number[] = []
        const optionCleaned: number[] = []
        const armed = {value: true}
        const mapped = source.map((item, _index, {onCleanup}) => {
            dep()
            onCleanup(() => {
                contextCleaned.push(item)
                if (armed.value && (item === 1 || item === 3)) throw new Error(`context cleanup ${item}`)
            })
            return item * 10
        }, {
            onCleanup: (value: number) => {
                optionCleaned.push(value)
                if (armed.value && value === 20) throw new Error(`option cleanup ${value}`)
            },
        })
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const reportedMessages = () => reported.mock.calls
            .filter(call => call[0] === '[data0] uncaught error in teardown:')
            .map(call => (call[1] as Error).message)
        return {dep, source, mapped, contextCleaned, optionCleaned, armed, reportedMessages}
    }
    test('destroy: every row is cleaned, the first error is thrown, the later ones are reported', () => {
        const {mapped, contextCleaned, optionCleaned, reportedMessages} = setup()
        expect(() => mapped.destroy()).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        expect(mapped.active).toBe(false)
        mapped.destroy()                                   // idempotent: nothing runs twice
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
    })
    test('full rebuild: every old row is cleaned exactly once, the first error is thrown, and the list recovers', () => {
        const {source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => mapped.recompute(true)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        // the next trigger rebuilds from the final source and does not clean the old rows a second time
        source.push(5)
        expect(mapped.data).toEqual([10, 20, 30, 40, 50])
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        // the rebuilt rows are live and get their own single cleanup
        armed.value = false
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4, 1, 2, 3, 4, 5])
        expect(optionCleaned).toEqual([10, 20, 30, 40, 10, 20, 30, 40, 50])
    })
    test('full rebuild that fails, then destroy: the already-cleaned rows are not cleaned again', () => {
        const {mapped, contextCleaned, optionCleaned} = setup()
        expect(() => mapped.recompute(true)).toThrow('context cleanup 1')
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
    })
    test('splice of several rows: every removed row is cleaned, the first error reaches the mutation caller', () => {
        const {source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => source.splice(0, 3)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3])
        expect(optionCleaned).toEqual([10, 20, 30])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        // error recovery: the next trigger rebuilds; the removed rows are not cleaned again
        armed.value = false
        source.push(5)
        expect(mapped.data).toEqual([40, 50])
        expect(contextCleaned).toEqual([1, 2, 3, 4])       // row 4 was torn down by the rebuild, once
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4, 4, 5])
        expect(optionCleaned).toEqual([10, 20, 30, 40, 40, 50])
    })
    test('set (single row): the option cleanup still runs after the context cleanup threw, and the new row is in place', () => {
        const {dep, source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => source.set(0, 7)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1])
        expect(optionCleaned).toEqual([10])
        expect(mapped.data).toEqual([70, 20, 30, 40])
        expect(reportedMessages()).toEqual([])
        // error recovery: the next trigger rebuilds; the replaced row is not cleaned again
        armed.value = false
        source.set(1, 8)
        expect(mapped.data).toEqual([70, 80, 30, 40])
        expect(contextCleaned).toEqual([1, 7, 2, 3, 4])   // rows 7, 2, 3, 4 torn down by the rebuild, once each
        expect(optionCleaned).toEqual([10, 70, 20, 30, 40])
        dep(1)
        expect(mapped.data).toEqual([70, 80, 30, 40])
    })
    test('set: both cleanups of the replaced row throw -> first thrown, second reported', () => {
        const source = new RxList<number>([1, 2])
        const order: string[] = []
        const mapped = source.map((item, _index, {onCleanup}) => {
            onCleanup(() => { order.push(`context ${item}`); if (item === 1) throw new Error('context boom') })
            return item * 10
        }, {onCleanup: (value: number) => { order.push(`option ${value}`); if (value === 10) throw new Error('option boom') }})
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        expect(() => source.set(0, 3)).toThrow('context boom')
        expect(order).toEqual(['context 1', 'option 10'])
        expect(reported.mock.calls.filter(call => call[0] === '[data0] uncaught error in teardown:').map(call => (call[1] as Error).message)).toEqual(['option boom'])
        expect(mapped.data).toEqual([30, 20])
        source.push(4)                                     // recovers; rows 3 and 2 are torn down by the rebuild exactly once
        expect(mapped.data).toEqual([30, 20, 40])
        expect(order).toEqual(['context 1', 'option 10', 'context 3', 'context 2', 'option 30', 'option 20'])
    })
    test('row effects: a child effect whose cleanup throws does not stop the teardown of the other removed rows', () => {
        const source = new RxList<number>([1, 2, 3])
        const childCleaned: number[] = []
        const contextCleaned: number[] = []
        const optionCleaned: number[] = []
        const mapped = source.map((item, _index, {onCleanup}) => {
            const child = computed(({onCleanup: onChildCleanup}) => {
                onChildCleanup(() => { childCleaned.push(item); if (item === 1) throw new Error('child cleanup boom') })
                return item
            })
            onCleanup(() => contextCleaned.push(item))
            return child
        }, {onCleanup: child => optionCleaned.push(child.raw)})
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        expect(() => source.splice(0, 2)).toThrow('child cleanup boom')
        expect(childCleaned).toEqual([1, 2])
        expect(contextCleaned).toEqual([1, 2])
        expect(optionCleaned).toEqual([1, 2])
        expect(mapped.data.length).toBe(1)
    })
})

describe('filter: indicator is committed before the filtered list changes (reactions during the dispatch see a consistent state)', () => {
    type Row = { id: number, on: Atom<boolean> }
    test('differential: a subscriber of the filtered list toggles other rows / mutates the source while a toggle is being dispatched', () => {
        for (let seed = 1; seed <= 150; seed++) {
            const rand = mulberry32(seed)
            let nextId = 1
            const pool: Row[] = []
            const mk = (): Row => { const row = {id: nextId++, on: atom(rand() < 0.5)}; pool.push(row); return row }
            const source = new RxList<Row>(Array.from({length: 6}, mk))
            const filtered = source.filter(row => row.on())
            const mirror = filtered.map(row => row.id)
            const history: string[] = []
            let budget = 0
            // rows whose indicator may be mid-recompute right now: writing their atom again from a
            // reaction is the documented synchronous recompute loop (README §2), not what is tested here
            const busy = new Set<Row>()
            const react = () => {
                if (budget <= 0) return
                budget--
                const r = rand()
                const len = source.data.length
                if (r < 0.5) {
                    const row = pool[Math.floor(rand() * pool.length)]
                    if (busy.has(row)) return
                    busy.add(row)
                    history.push(`  nested flip #${row.id}`); row.on(!row.on.raw)
                }
                else if (r < 0.65) { const row = mk(); history.push(`  nested push #${row.id}`); source.push(row) }
                else if (r < 0.8 && len) { const i = Math.floor(rand() * len); history.push(`  nested splice(${i},1)`); source.splice(i, 1) }
                else if (r < 0.9 && len >= 2) { history.push('  nested swap(0,last)'); source.swap(0, len - 1) }
                else if (len) { const i = Math.floor(rand() * len); const row = mk(); history.push(`  nested set(${i},#${row.id})`); source.set(i, row) }
            }
            const stop = onChange(filtered, react)
            for (let step = 0; step < 60; step++) {
                budget = Math.floor(rand() * 3)
                busy.clear()
                const row = pool[Math.floor(rand() * pool.length)]
                const r = rand()
                if (r < 0.7) { history.push(`flip #${row.id}`); busy.add(row); row.on(!row.on.raw) }
                else if (r < 0.85) { history.push('batch flips'); batch(() => { for (let i = 0; i < 3; i++) { const x = pool[Math.floor(rand() * pool.length)]; busy.add(x); x.on(!x.on.raw) } }) }
                else { const fresh = mk(); history.push(`unshift #${fresh.id}`); source.unshift(fresh) }
                const model = source.data.filter(x => x.on.raw).map(x => x.id)
                const ctx = `seed ${seed} step ${step}\n${history.join('\n')}`
                expect(filtered.data.map(x => x.id), ctx).toEqual(model)
                expect(mirror.data, ctx).toEqual(model)
            }
            stop(); mirror.destroy(); filtered.destroy(); source.destroy()
        }
    })
    test('a subscriber that throws during the dispatch leaves indicator and filtered list in step', () => {
        const a = {id: 'a', on: atom(false)}, b = {id: 'b', on: atom(true)}
        const source = new RxList([a, b])
        const filtered = source.filter(row => row.on())
        let boom = true
        const stop = onChange(filtered, () => { if (boom) { boom = false; throw new Error('subscriber boom') } })
        expect(() => a.on(true)).toThrow('subscriber boom')
        expect(filtered.data.map(row => row.id)).toEqual(['a', 'b'])
        // later structural changes still line up (the prefix count uses the committed indicator)
        source.shift()
        expect(filtered.data.map(row => row.id)).toEqual(['b'])
        source.unshift(a)
        a.on(false)
        expect(filtered.data.map(row => row.id)).toEqual(['b'])
        stop()
    })
})

describe('filter: one shared predicate dependency flipping many rows', () => {
    const flipAll = (n: number, shuffled: boolean) => {
        const threshold = atom(-1)
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const filtered = source.filter(x => x > threshold())
        if (shuffled) {
            // rows no longer sit in subscription order
            const rand = mulberry32(n)
            const keys = new Map<number, number>(source.data.map(x => [x, rand()]))
            source.sortSelf((a, b) => keys.get(a)! - keys.get(b)!)
        }
        const off = time(() => { threshold(n) })
        expect(filtered.data).toEqual([])
        const on = time(() => { threshold(-1) })
        expect(filtered.data).toEqual(source.data)
        const half = time(() => { threshold(n / 2) })
        expect(filtered.data).toEqual(source.data.filter(x => x > n / 2))
        filtered.destroy(); source.destroy()
        return {off, on, half}
    }
    test('differential: waves of flips x structural changes x reorders x batches', () => {
        for (let seed = 1; seed <= 120; seed++) {
            const rand = mulberry32(seed)
            let next = 0
            const threshold = atom(3)
            const parity = atom(0)
            const pred = (x: number) => x > threshold() && (x + parity()) % 3 !== 0
            const source = new RxList<number>(Array.from({length: 150}, () => next++))   // longer than the locate window
            const filtered = source.filter(pred)
            const mirror = filtered.map(x => x)
            const history: string[] = []
            const op = () => {
                const len = source.data.length, r = rand()
                if (r < 0.30) { const v = Math.floor(rand() * (next + 2)) - 1; history.push(`threshold=${v}`); threshold(v) }
                else if (r < 0.45) { const v = Math.floor(rand() * 3); history.push(`parity=${v}`); parity(v) }
                else if (r < 0.55) { history.push('push'); source.push(next++) }
                else if (r < 0.62) { history.push('unshift'); source.unshift(next++) }
                else if (r < 0.72 && len) { const i = Math.floor(rand() * len), d = Math.floor(rand() * 4); history.push(`splice(${i},${d},+1)`); source.splice(i, d, next++) }
                else if (r < 0.80 && len) { const i = Math.floor(rand() * len); history.push(`set(${i})`); source.set(i, next++) }
                else if (r < 0.88 && len >= 2) { const a = Math.floor(rand() * (len - 1)); history.push(`swap(${a},${len - 1})`); source.swap(a, len - 1) }
                else if (r < 0.94 && len >= 2) { history.push('sortSelf(desc)'); source.sortSelf((a, b) => b - a) }
                else if (len >= 2) { history.push('sortSelf(by x*7%11)'); source.sortSelf((a, b) => (a * 7) % 11 - (b * 7) % 11) }
            }
            for (let step = 0; step < 40; step++) {
                if (rand() < 0.3) { history.push('batch{'); batch(() => { op(); op(); op() }); history.push('}') }
                else op()
                const model = source.data.filter(x => x > threshold.raw && (x + parity.raw) % 3 !== 0)
                const ctx = `seed ${seed} step ${step}: ${history.slice(-12).join(' | ')}`
                expect(filtered.data, ctx).toEqual(model)
                expect(mirror.data, ctx).toEqual(model)
            }
            mirror.destroy(); filtered.destroy(); source.destroy()
        }
    })
    test('timings at 10k and 40k rows (rows in subscription order, and shuffled)', () => {
        flipAll(2000, false) // warm up
        const rows: string[] = []
        const result: Record<string, {off: number, on: number, half: number}> = {}
        for (const shuffled of [false, true]) {
            // shuffled rows stay quadratic (every locate falls back to the scan from row 0): smaller sizes keep the test short
            for (const n of shuffled ? [5000, 10000] : [10000, 40000]) {
                const r = flipAll(n, shuffled)
                result[`${shuffled}:${n}`] = r
                rows.push(`${shuffled ? 'shuffled' : 'in order'} n=${n}: all off ${r.off.toFixed(1)}ms, all on ${r.on.toFixed(1)}ms, half off ${r.half.toFixed(1)}ms`)
            }
        }
        console.log('FILTER SHARED-DEP TIMINGS\n  ' + rows.join('\n  '))
        // in subscription order every row is located from the previous one: turning all rows on is linear
        expect(result['false:40000'].on / result['false:10000'].on).toBeLessThan(10)
    }, 240000)
})

describe('map: reactive rows recomputing while a structural change is still pending', () => {
    test('timings: batch(() => { sharedDep(x); source.push(y) }) at 5k and 40k rows', () => {
        const run = (n: number) => {
            const factor = atom(1)
            const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
            const mapped = source.map(x => x * factor())
            const plain = time(() => { factor(2) })
            const batched = time(() => { batch(() => { factor(3); source.push(1) }) })
            expect(mapped.data).toEqual(source.data.map(x => x * 3))
            // deferred map: the structural info waits in the scheduler while every row recomputes
            const pending: (() => void)[] = []
            const deferred = source.map(x => x + factor(), {scheduleRecompute: (run: any) => { pending.push(run) }})
            source.unshift(7)
            const whilePending = time(() => { factor(4) })
            pending.splice(0).forEach(r => r())
            expect(deferred.data).toEqual(source.data.map(x => x + 4))
            mapped.destroy(); deferred.destroy(); source.destroy()
            return {plain, batched, whilePending}
        }
        run(1000) // warm up
        const small = run(5000), large = run(40000)
        console.log(`MAP PENDING TIMINGS\n  5k: plain atom write ${small.plain.toFixed(1)}ms, batch(atom, push) ${small.batched.toFixed(1)}ms, atom write with a pending info ${small.whilePending.toFixed(1)}ms`
            + `\n  40k: plain atom write ${large.plain.toFixed(1)}ms, batch(atom, push) ${large.batched.toFixed(1)}ms, atom write with a pending info ${large.whilePending.toFixed(1)}ms`)
        expect(large.batched / small.batched).toBeLessThan(24)          // linear would be ~8
        expect(large.whilePending / small.whilePending).toBeLessThan(24)
    }, 120000)
    test('a row moved by the running patch is still found at its current position (the position cache validates itself)', () => {
        // deferred map patched outside any digest: writes made by mapFn / cleanups during the patch
        // trigger the surviving rows inline, before and after the row bookkeeping is spliced
        const pending: (() => void)[] = []
        const flush = () => pending.splice(0).forEach(run => run())
        const x = atom(0)
        const source = new RxList<string>(['r0', 'r1', 'r2'])
        const mapped = source.map((item, _index, {onCleanup}) => {
            if (item.startsWith('n')) x(x.raw + 1)                    // a new row announces itself: older rows recompute inline
            onCleanup(() => { if (item === 'r0') x(x.raw + 1) })      // ...and again when the removed row is cleaned
            return item.startsWith('r') ? `${item}:${x()}` : item
        }, {scheduleRecompute: (run: any) => { pending.push(run) }})
        expect(mapped.data).toEqual(['r0:0', 'r1:0', 'r2:0'])
        source.splice(0, 1, 'n1', 'n2')                                // r1, r2 shift from 1, 2 to 2, 3
        flush(); flush()
        expect(mapped.data).toEqual(['n1', 'n2', `r1:${x.raw}`, `r2:${x.raw}`])
        x(x.raw + 1)
        expect(mapped.data).toEqual(['n1', 'n2', `r1:${x.raw}`, `r2:${x.raw}`])
    })
})

describe('map options: every patch round that called beforePatch also calls afterPatch', () => {
    const setup = (mapFn: (x: number | 'live', index?: any) => number, initial: (number | 'live')[]) => {
        const source = new RxList<number | 'live'>(initial)
        const log: string[] = []
        const mapped = source.map(mapFn as any, {
            beforePatch: info => { log.push(`before:${info.method ?? 'set'}`) },
            afterPatch: infos => { log.push(`after:${infos.length}`) },
        })
        mapped.on('fullRecompute', () => log.push('fullRecompute'))
        return {source, mapped, log}
    }
    const live = atom(5)
    test('normal patch rounds (single and multi info)', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        source.push(3)
        batch(() => { source.push(4); source.shift() })
        expect(log).toEqual(['before:splice', 'after:1', 'before:splice', 'before:splice', 'after:2'])
        expect(mapped.data).toEqual([2, 3, 4])
    })
    test('round that bails out to a full recompute because the first reactive row appeared mid-replay', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        batch(() => { source.push('live'); source.push(3) })
        expect(log).toEqual(['before:splice', 'before:splice', 'after:2', 'fullRecompute'])
        expect(mapped.data).toEqual([1, 2, 5, 3])
    })
    test('round that bails out because an operation-time index is beyond the final source', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        batch(() => { source.push('live'); source.splice(0, 3) })
        expect(log).toEqual(['before:splice', 'before:splice', 'after:2', 'fullRecompute'])
        expect(mapped.data).toEqual([])
    })
    test('round rejected before any info is replayed (index-atom map, multi info): neither hook runs', () => {
        const {source, mapped, log} = setup((x, index) => (x as number) + index(), [1, 2])
        batch(() => { source.push(3); source.push(4) })
        expect(log).toEqual(['fullRecompute'])
        expect(mapped.data).toEqual([1, 3, 5, 7])
    })
})
