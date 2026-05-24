import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DoktorABC Sales",
  description: "Verkaufsdashboard fuer DoktorABC Abrechnungsdaten.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="de">
      <body>{children}</body>
    </html>
  );
}
