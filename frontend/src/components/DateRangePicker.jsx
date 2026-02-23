/**
 * DateRangePicker Component
 *
 * Two date inputs (Start Date, End Date) + Calculate Payouts button + Loading state.
 * Layout: date inputs side by side, button to the right, spinner next to button.
 *
 * Props:
 *   startDate    — current start date value (string "YYYY-MM-DD")
 *   endDate      — current end date value (string "YYYY-MM-DD")
 *   onStartChange — callback when start date changes
 *   onEndChange   — callback when end date changes
 *   onSubmit      — callback when Calculate button is clicked
 *   isLoading     — whether the pipeline is running
 *   statusText    — loading status text ("Fetching data...", etc.)
 */

export default function DateRangePicker({
  startDate,
  endDate,
  onStartChange,
  onEndChange,
  onSubmit,
  isLoading,
  statusText,
}) {
  return (
    <div className="flex flex-wrap items-end justify-center gap-4">
      {/* ---- Start Date ---- */}
      <div className="flex flex-col">
        <label
          htmlFor="start-date"
          className="mb-1.5 text-xs font-medium tracking-wide text-poly-gray-500 uppercase"
        >
          Start Date
        </label>
        <input
          id="start-date"
          type="date"
          value={startDate}
          onChange={(e) => onStartChange(e.target.value)}
          disabled={isLoading}
          className="h-10 w-44 rounded-lg border border-poly-gray-200 bg-white px-3 text-sm
                     text-poly-gray-900 shadow-sm outline-none
                     focus:border-poly-blue focus:ring-2 focus:ring-poly-blue/20
                     disabled:bg-poly-gray-50 disabled:text-poly-gray-400 disabled:cursor-not-allowed
                     transition-all"
        />
      </div>

      {/* ---- End Date ---- */}
      <div className="flex flex-col">
        <label
          htmlFor="end-date"
          className="mb-1.5 text-xs font-medium tracking-wide text-poly-gray-500 uppercase"
        >
          End Date
        </label>
        <input
          id="end-date"
          type="date"
          value={endDate}
          onChange={(e) => onEndChange(e.target.value)}
          disabled={isLoading}
          className="h-10 w-44 rounded-lg border border-poly-gray-200 bg-white px-3 text-sm
                     text-poly-gray-900 shadow-sm outline-none
                     focus:border-poly-blue focus:ring-2 focus:ring-poly-blue/20
                     disabled:bg-poly-gray-50 disabled:text-poly-gray-400 disabled:cursor-not-allowed
                     transition-all"
        />
      </div>

      {/* ---- Calculate Button ---- */}
      <button
        onClick={onSubmit}
        disabled={isLoading}
        className="h-10 px-6 rounded-lg text-sm font-semibold text-white shadow-sm
                   bg-poly-blue hover:bg-poly-blue-dark active:bg-poly-blue-dark
                   disabled:bg-poly-gray-300 disabled:cursor-not-allowed
                   transition-colors cursor-pointer"
      >
        {isLoading ? "Processing..." : "Calculate Payouts"}
      </button>

      {/* ---- Loading Spinner + Status Text ---- */}
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-poly-gray-500">
          {/* SVG Spinner */}
          <svg
            className="spinner h-4 w-4 text-poly-blue"
            viewBox="0 0 24 24"
            fill="none"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          <span>{statusText}</span>
        </div>
      )}
    </div>
  );
}
