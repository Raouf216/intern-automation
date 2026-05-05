import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DoktorABC Produktsynchronisierung",
  description: "Operative Steuerungsoberfläche für die DoktorABC Produktsynchronisierung.",
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
