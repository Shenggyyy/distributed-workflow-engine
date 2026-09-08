// All intervals are bounded by real samples. Never extend them with Date.now().
export function intervals(samples) {
  const groups = new Map();
  for (const sample of samples) {
    if (!groups.has(sample.invocation_id)) groups.set(sample.invocation_id, []);
    groups.get(sample.invocation_id).push(sample);
  }
  return [...groups.values()].map(group => {
    group.sort((a, b) => a.sequence - b.sequence);
    const first = group[0],
      last = group.at(-1);
    return {
      id: first.invocation_id,
      attempt: first.attempt_id,
      domain: first.clock_domain,
      start: BigInt(first.monotonic_ns),
      end: BigInt(last.monotonic_ns),
      finished: last.phase === 'FINISH',
      count: group.length
    };
  });
}

export function peakOverlap(values) {
  const domains = new Map();
  for (const value of values) {
    if (value.end <= value.start) continue;
    if (!domains.has(value.domain)) domains.set(value.domain, []);
    domains.get(value.domain).push([value.start, 1], [value.end, -1]);
  }
  let peak = 0;
  for (const events of domains.values()) {
    events.sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] - b[1]);
    let active = 0;
    for (const [, delta] of events) {
      active += delta;
      peak = Math.max(peak, active);
    }
  }
  return peak;
}
