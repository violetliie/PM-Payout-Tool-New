/**
 * EmptyState Component
 *
 * Shown when the calculation completes but no videos were found.
 * Clean, informative message with Polymarket styling.
 */

export default function EmptyState() {
  return (
    <div className="mx-auto mt-8 max-w-md text-center">
      <div className="rounded-xl border border-poly-gray-200 bg-poly-gray-50 px-6 py-10">
        {/* ---- Empty Icon ---- */}
        <svg
          className="mx-auto h-12 w-12 text-poly-gray-300"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={1}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z"
          />
        </svg>

        <h3 className="mt-4 text-base font-semibold text-poly-gray-700">
          No Videos Found
        </h3>
        <p className="mt-2 text-sm text-poly-gray-500">
          No videos were found for the selected date range.
          Try adjusting the start and end dates.
        </p>
      </div>
    </div>
  );
}
