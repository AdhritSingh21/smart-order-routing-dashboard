import type { ReactNode } from 'react'

interface PanelHeaderProps {
  eyebrow: string
  title: string
  aside?: ReactNode
}

export function PanelHeader({ eyebrow, title, aside }: PanelHeaderProps) {
  return (
    <header className="panel-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h2>{title}</h2>
      </div>
      {aside}
    </header>
  )
}
