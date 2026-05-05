import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Rats-Apotheke Notifications",
  description: "Notification center for pharmacy operations.",
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
