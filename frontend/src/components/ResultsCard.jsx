/**
 * ResultsCard Component
 *
 * Displays the calculation results in a card with Polymarket styling.
 * Shows summary stats + "Download Report" button.
 *
 * Props:
 *   summary  — { total_creators, total_payout, total_videos, paired_count,
 *                unpaired_count, exception_count }
 *   filename — the .xlsx filename for download
 *   onDownload — callback to trigger file download
 */

export default function ResultsCard({ summary, filename, onDownload }) {
  if (!summary) return null;

  const stats = [
    {
      label: "Total Creators",
      value: summary.total_creators?.toLocaleString() ?? "0",
      icon: (
        <svg className="h-5 w-5 text-poly-blue" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15 19.128a9.38 9.38 0 002.625.372 9.337 9.337 0 004.121-.952 4.125 4.125 0 00-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 018.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0111.964-3.07M12 6.375a3.375 3.375 0 11-6.75 0 3.375 3.375 0 016.75 0zm8.25 2.25a2.625 2.625 0 11-5.25 0 2.625 2.625 0 015.25 0z" />
        </svg>
      ),
    },
    {
      label: "Total Payout",
      value: `$${(summary.total_payout ?? 0).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`,
      icon: (
        <svg className="h-5 w-5 text-poly-green" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v12m-3-2.818l.879.659c1.171.879 3.07.879 4.242 0 1.172-.879 1.172-2.303 0-3.182C13.536 12.219 12.768 12 12 12c-.725 0-1.45-.22-2.003-.659-1.106-.879-1.106-2.303 0-3.182s2.9-.879 4.006 0l.415.33M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
      highlight: true,
    },
    {
      label: "Videos Processed",
      value: summary.total_videos_processed?.toLocaleString() ?? "0",
      icon: (
        <svg className="h-5 w-5 text-poly-blue" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="m15.75 10.5 4.72-4.72a.75.75 0 0 1 1.28.53v11.38a.75.75 0 0 1-1.28.53l-4.72-4.72M4.5 18.75h9a2.25 2.25 0 0 0 2.25-2.25v-9a2.25 2.25 0 0 0-2.25-2.25h-9A2.25 2.25 0 0 0 2.25 7.5v9a2.25 2.25 0 0 0 2.25 2.25Z" />
        </svg>
      ),
    },
    {
      label: "Paired",
      value: summary.total_paired?.toLocaleString() ?? "0",
      icon: (
        <svg className="h-5 w-5 text-poly-green" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
    },
    {
      label: "Unpaired",
      value: summary.total_unpaired?.toLocaleString() ?? "0",
      icon: (
        <svg className="h-5 w-5 text-poly-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 9.75l4.5 4.5m0-4.5l-4.5 4.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      ),
    },
    {
      label: "Exceptions",
      value: summary.total_exceptions?.toLocaleString() ?? "0",
      icon: (
        <svg className="h-5 w-5 text-poly-red" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
        </svg>
      ),
    },
  ];

  return (
    <div className="mx-auto mt-8 max-w-2xl animate-in">
      {/* ---- Card Container ---- */}
      <div className="rounded-xl border border-poly-gray-200 bg-white shadow-lg overflow-hidden">
        {/* ---- Card Header ---- */}
        <div className="border-b border-poly-gray-100 bg-poly-gray-50 px-6 py-4">
          <h2 className="text-lg font-semibold text-poly-gray-900">
            Payout Summary
          </h2>
        </div>

        {/* ---- Stats Grid ---- */}
        <div className="grid grid-cols-2 gap-px bg-poly-gray-100 sm:grid-cols-3">
          {stats.map((stat) => (
            <div
              key={stat.label}
              className="flex flex-col items-center gap-1 bg-white px-4 py-5"
            >
              {stat.icon}
              <span
                className={`text-xl font-bold ${
                  stat.highlight ? "text-poly-green" : "text-poly-gray-900"
                }`}
              >
                {stat.value}
              </span>
              <span className="text-xs text-poly-gray-500">{stat.label}</span>
            </div>
          ))}
        </div>

        {/* ---- Download Button ---- */}
        <div className="flex justify-end border-t border-poly-gray-100 bg-poly-gray-50 px-6 py-4">
          <button
            onClick={onDownload}
            className="flex items-center gap-2 rounded-lg bg-poly-blue px-5 py-2.5
                       text-sm font-semibold text-white shadow-sm
                       hover:bg-poly-blue-dark active:bg-poly-blue-dark
                       transition-colors cursor-pointer"
          >
            {/* Download Icon */}
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
            </svg>
            Download Report
          </button>
        </div>
      </div>
    </div>
  );
}
