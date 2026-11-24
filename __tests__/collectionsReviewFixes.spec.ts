/**
 * Regression tests for the review of the collection fixes (PR #24, commit 6b52f72 follow-ups).
 * Each describe block belongs to one root cause; every test failed on the tree before its fix.
 */
import {describe, expect, test} from 'vitest'
import {batch, onChange, RxList, RxMap, RxSet} from '../src/index'
import {Computed} from '../src/computed.js'
import {ITERATE_KEY} from '../src/notify.js'
import {TrackOpTypes} from '../src/operations.js'

const zeroSign = (x: unknown) => Object.is(x, -0) ? '-0' : String(x)
const sameSeq = (a: readonly unknown[], b: readonly unknown[]) => a.length === b.length && a.every((x, i) => Object.is(x, b[i]))

// ---------------------------------------------------------------------------
// -0: trigger payloads / argv stay what the caller passed (external protocol, AGENTS: internal consumers
// normalize); the incremental consumers that rebuild a key sequence from the payload normalize themselves
// ---------------------------------------------------------------------------
describe('-0 keys: raw payloads, normalizing internal consumers', () => {
    const addKeyOf = (write: (m: RxMap<number, string>) => void) => {
        const m = new RxMap<number, string>([])
        let key: unknown = 'none'
        const c = new Computed(function (this: Computed) { this.manualTrack(m, TrackOpTypes.ITERATE, ITERATE_KEY) },
            function (_d: unknown, infos: any[]) { for (const i of infos) if (i.type === 'add') key = i.key })
        c.run([], true)
        write(m)
        c.destroy()
        return zeroSign(key)
    }

    test('RxMap.set / delete and RxSet.add / delete: argv carries the raw -0', () => {
        const m = new RxMap<number, string>([])
        const s = new RxSet<number>([])
        const argv: string[] = []
        const stopM = onChange(m, (infos: any[]) => { for (const i of infos) argv.push(`map.${i.method}(${zeroSign(i.argv[0])})`) })
        const stopS = onChange(s, (infos: any[]) => { for (const i of infos) argv.push(`set.${i.method}(${zeroSign(i.argv[0])})`) })
        m.set(-0, 'a'); m.delete(-0)
        s.add(-0); s.delete(-0)
        stopM(); stopS()
        expect(argv).toEqual(['map.set(-0)', 'map.delete(-0)', 'set.add(-0)', 'set.delete(-0)'])
    })

    test('set() and replace() / replaceData() emit the same ADD key for -0', () => {
        expect([addKeyOf(m => m.set(-0, 'v')), addKeyOf(m => m.replace([[-0, 'v']])), addKeyOf(m => m.replaceData([[-0, 'v']]))])
            .toEqual(['-0', '-0', '-0'])
    })

    test('consumers rebuilt from raw payloads hold the stored +0, directly and through derived maps / sets, in and out of batch', () => {
        const m = new RxMap<number, string>([[1, 'one']])
        const keys = m.keys(), entries = m.entries()
        const s = new RxSet<number>([1])
        const members = s.toList()
        const unionMembers = s.union(new RxSet<number>([5])).toList()
        const list = new RxList<number>([1])
        const indexKeys = list.map(x => ({id: x})).indexBy('id').keys()
        const groupEntryKeys = list.groupBy(x => x).entries().map(([k]) => k)
        const setMembers = list.toSet().toList()

        m.set(-0, 'z'); s.add(-0); list.push(-0)
        batch(() => { m.delete(1); m.set(-0, 'z2'); m.set(2, 'two'); s.delete(1); s.add(2) })

        expect(sameSeq(keys.data, [...m.data.keys()])).toBe(true)
        expect(sameSeq(entries.data.map(e => e[0]), [...m.data.keys()])).toBe(true)
        expect(sameSeq(members.data, [...s.data])).toBe(true)
        expect(unionMembers.data.some(x => Object.is(x, -0))).toBe(false)
        for (const derived of [indexKeys, groupEntryKeys, setMembers]) {
            expect(derived.data.some(x => Object.is(x, -0)), 'derived key sequence holds -0').toBe(false)
            expect(derived.data.some(x => Object.is(x, 0))).toBe(true)
        }
    })
})
