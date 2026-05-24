import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DoktorABC Sales",
  description: "Verkaufsdashboard fuer DoktorABC Abrechnungsdaten.",
  icons: {
    icon: "/icon.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="de">
      <body>
        <script
          dangerouslySetInnerHTML={{
            __html:
              "(function(){try{var theme=localStorage.getItem('sales-dashboard-theme');if(theme==='dark'||theme==='light'){document.documentElement.dataset.theme=theme;document.documentElement.style.colorScheme=theme;}}catch(error){}})();",
          }}
        />
        {children}
      </body>
    </html>
  );
}
