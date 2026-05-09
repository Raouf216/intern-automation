import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Self Pickup Abholstatus",
  description: "Operative Steuerungsoberfläche für Self Pickup Abholungen.",
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
