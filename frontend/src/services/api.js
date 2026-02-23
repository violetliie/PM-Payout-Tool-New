/**
 * API Service Layer
 *
 * Handles all communication with the FastAPI backend.
 * The backend URL is configurable via VITE_API_URL environment variable.
 *
 * Endpoints:
 *   POST /api/calculate  — Run the payout pipeline
 *   GET  /api/download/{filename} — Download the generated .xlsx report
 */

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ===========================================================================
// Calculate payouts
// ===========================================================================

/**
 * Trigger the payout calculation pipeline.
 *
 * @param {string} startDate  — "YYYY-MM-DD"
 * @param {string} endDate    — "YYYY-MM-DD"
 * @returns {Promise<Object>} — { status, filename, summary }
 * @throws {Error} with descriptive message on failure
 */
export async function calculatePayouts(startDate, endDate) {
  const url = `${API_URL}/api/calculate`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      start_date: startDate,
      end_date: endDate,
    }),
  });

  if (!response.ok) {
    // Try to parse error detail from FastAPI
    let errorMessage = `Server error (${response.status})`;
    try {
      const errorData = await response.json();
      if (errorData.detail) {
        errorMessage = errorData.detail;
      }
    } catch {
      // Response body wasn't JSON — use status text
      errorMessage = `${response.status}: ${response.statusText}`;
    }
    throw new Error(errorMessage);
  }

  return response.json();
}

// ===========================================================================
// Download report
// ===========================================================================

/**
 * Download the generated .xlsx report.
 *
 * Creates a temporary <a> tag to trigger the browser's native download dialog.
 *
 * @param {string} filename — The filename returned by /api/calculate
 */
export function downloadReport(filename) {
  const url = `${API_URL}/api/download/${encodeURIComponent(filename)}`;
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
