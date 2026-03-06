interface Props {
  number: string;
  label: string;
}

export default function SectionLabel({ number, label }: Props) {
  return (
    <div className="flex items-center gap-3 mb-4">
      <span className="text-xs font-mono text-[var(--accent)] tracking-widest">{number}</span>
      <div className="h-px w-8 bg-[var(--accent)] opacity-60" />
      <span className="text-xs font-medium tracking-widest uppercase text-[var(--text-secondary)]">{label}</span>
    </div>
  );
}
