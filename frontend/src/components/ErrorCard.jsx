/**
 * ErrorCard Component
 *
 * Displays error messages in a red-themed card.
 * Parses the error message to highlight specific failure reasons
 * (Google Sheet, API, validation, etc.)
 *
 * Props:
 *   message   — the error message string
 *   onDismiss — callback to close the error card
 */

export default function ErrorCard({ message, onDismiss }) {
  if (!message) return null;

  // ---- Determine error category for better UX ----
  const lowerMsg = message.toLowerCase();
  let errorTitle = "Calculation Failed";
  let errorIcon = "exclamation";

  if (lowerMsg.includes("google") || lowerMsg.includes("sheet") || lowerMsg.includes("creator mapping")) {
    errorTitle = "Creator Sheet Error";
  } else if (lowerMsg.includes("shortimize") || lowerMsg.includes("api") || lowerMsg.includes("fetch")) {
    errorTitle = "API Connection Error";
  } else if (lowerMsg.includes("date") || lowerMsg.includes("valid")) {
    errorTitle = "Validation Error";
    errorIcon = "info";
  } else if (lowerMsg.includes("network") || lowerMsg.includes("connect") || lowerMsg.includes("refused")) {
    errorTitle = "Network Error";
  }

  return (
    <div className="mx-auto mt-6 max-w-2xl">
      <div className="rounded-xl border border-red-200 bg-red-50 p-5 shadow-sm">
        <div className="flex items-start gap-3">
          {/* ---- Error Icon ---- */}
          <div className="flex-shrink-0 mt-0.5">
            <svg
              className="h-5 w-5 text-red-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.5}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z"
              />
            </svg>
          </div>

          {/* ---- Error Content ---- */}
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-red-800">
              {errorTitle}
            </h3>
            <p className="mt-1 text-sm text-red-700">
              {message}
            </p>
          </div>

          {/* ---- Dismiss Button ---- */}
          <button
            onClick={onDismiss}
            className="flex-shrink-0 rounded-md p-1 text-red-400 hover:text-red-600
                       hover:bg-red-100 transition-colors cursor-pointer"
            aria-label="Dismiss error"
          >
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
}
