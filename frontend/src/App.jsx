/**
 * App — Main Application Component
 *
 * Polymarket Creator Payout Tool: single-page app that lets users
 * select a date range, trigger the payout pipeline, view results,
 * and download the .xlsx report.
 *
 * State machine:
 *   idle      → User sets dates and clicks "Calculate"
 *   loading   → Pipeline is running; spinner + status text shown
 *   success   → ResultsCard displayed with summary + download button
 *   empty     → Pipeline returned no videos
 *   error     → ErrorCard displayed with failure message
 */

import { useState } from "react";
import Header from "./components/Header";
import DateRangePicker from "./components/DateRangePicker";
import ResultsCard from "./components/ResultsCard";
import ErrorCard from "./components/ErrorCard";
import EmptyState from "./components/EmptyState";
import { calculatePayouts, downloadReport } from "./services/api";

// ---- Helpers ----

/** Get today's date as "YYYY-MM-DD" */
function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

/** Get a date N days ago as "YYYY-MM-DD" */
function daysAgoStr(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

// ===========================================================================
// App Component
// ===========================================================================

export default function App() {
  // ---- Form State ----
  const [startDate, setStartDate] = useState(daysAgoStr(7));
  const [endDate, setEndDate] = useState(todayStr());

  // ---- Pipeline State ----
  const [isLoading, setIsLoading] = useState(false);
  const [statusText, setStatusText] = useState("");

  // ---- Result State ----
  const [summary, setSummary] = useState(null);
  const [filename, setFilename] = useState(null);
  const [isEmpty, setIsEmpty] = useState(false);

  // ---- Error State ----
  const [error, setError] = useState(null);

  // ===========================================================================
  // Handle Calculate
  // ===========================================================================

  async function handleCalculate() {
    // ---- Client-side validation ----
    if (!startDate || !endDate) {
      setError("Please select both a start date and an end date.");
      return;
    }

    if (startDate > endDate) {
      setError("Start date must be on or before the end date.");
      return;
    }

    // ---- Reset state ----
    setError(null);
    setSummary(null);
    setFilename(null);
    setIsEmpty(false);
    setIsLoading(true);

    try {
      // Phase 1: Fetching
      setStatusText("Fetching data from Shortimize API...");

      const data = await calculatePayouts(startDate, endDate);

      // Phase 2: Check results
      setStatusText("Processing complete!");

      if (
        data.summary &&
        data.summary.total_videos_processed === 0 &&
        data.summary.total_creators === 0
      ) {
        setIsEmpty(true);
      } else {
        setSummary(data.summary);
        setFilename(data.filename);
      }
    } catch (err) {
      setError(err.message || "An unexpected error occurred.");
    } finally {
      setIsLoading(false);
      setStatusText("");
    }
  }

  // ===========================================================================
  // Handle Download
  // ===========================================================================

  function handleDownload() {
    if (filename) {
      downloadReport(filename);
    }
  }

  // ===========================================================================
  // Render
  // ===========================================================================

  return (
    <div className="min-h-screen bg-white">
      {/* ---- Header ---- */}
      <Header />

      {/* ---- Main Content ---- */}
      <main className="mx-auto max-w-3xl px-4 pb-16">
        {/* ---- Date Range Picker + Calculate Button ---- */}
        <DateRangePicker
          startDate={startDate}
          endDate={endDate}
          onStartChange={setStartDate}
          onEndChange={setEndDate}
          onSubmit={handleCalculate}
          isLoading={isLoading}
          statusText={statusText}
        />

        {/* ---- Error Card ---- */}
        <ErrorCard message={error} onDismiss={() => setError(null)} />

        {/* ---- Empty State ---- */}
        {isEmpty && <EmptyState />}

        {/* ---- Results Card ---- */}
        <ResultsCard
          summary={summary}
          filename={filename}
          onDownload={handleDownload}
        />
      </main>

      {/* ---- Footer ---- */}
      <footer className="py-6 text-center text-xs text-poly-gray-400">
        Polymarket Creator Payout Tool &middot; Internal Use Only
      </footer>
    </div>
  );
}
