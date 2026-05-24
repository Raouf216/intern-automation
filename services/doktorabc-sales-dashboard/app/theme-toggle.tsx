"use client";

import { Moon, Sun } from "lucide-react";
import { useEffect, useState } from "react";

type Theme = "dark" | "light";

function applyTheme(theme: Theme) {
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
  localStorage.setItem("sales-dashboard-theme", theme);
}

export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>("light");

  useEffect(() => {
    setTheme(document.documentElement.dataset.theme === "dark" ? "dark" : "light");
  }, []);

  function toggleTheme() {
    const nextTheme = theme === "dark" ? "light" : "dark";
    applyTheme(nextTheme);
    setTheme(nextTheme);
  }

  return (
    <button className="theme-toggle" onClick={toggleTheme} type="button" aria-label={theme === "dark" ? "Tagmodus aktivieren" : "Nachtmodus aktivieren"}>
      {theme === "dark" ? <Sun size={18} /> : <Moon size={18} />}
      <span>{theme === "dark" ? "Tag" : "Nacht"}</span>
    </button>
  );
}
