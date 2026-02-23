/**
 * Header Component
 *
 * Displays the Polymarket logo and tool title centered on the page.
 * Matches Polymarket's clean, minimal branding.
 */

import polymarketLogo from "../assets/polymarket-logo.png";

export default function Header() {
  return (
    <header className="pt-10 pb-6 text-center">
      {/* ---- Logo ---- */}
      <div className="flex justify-center mb-4">
        <img
          src={polymarketLogo}
          alt="Polymarket"
          className="h-10"
        />
      </div>

      {/* ---- Title ---- */}
      <h1 className="text-2xl font-semibold tracking-tight text-poly-gray-900">
        Creator Payout Tool
      </h1>
      <p className="mt-1 text-sm text-poly-gray-400">
        Calculate and export creator payouts for video campaigns
      </p>
    </header>
  );
}
