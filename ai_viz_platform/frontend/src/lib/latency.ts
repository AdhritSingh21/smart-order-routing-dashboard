export interface HistogramBin {
  label: string
  count: number
}

const BOUNDS = [2, 5, 10, 25, 50, 100]

export function buildLatencyHistogram(samples: number[]): HistogramBin[] {
  const bins: HistogramBin[] = [
    { label: '<2', count: 0 },
    { label: '2–5', count: 0 },
    { label: '5–10', count: 0 },
    { label: '10–25', count: 0 },
    { label: '25–50', count: 0 },
    { label: '50–100', count: 0 },
    { label: '100+', count: 0 },
  ]

  for (const sample of samples) {
    if (!Number.isFinite(sample) || sample < 0) continue
    const index = BOUNDS.findIndex((bound) => sample < bound)
    bins[index === -1 ? bins.length - 1 : index].count += 1
  }

  return bins
}
