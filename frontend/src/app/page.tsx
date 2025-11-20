"use client";

import dynamic from "next/dynamic";

const RainMap = dynamic(() => import("../components/Map"), {
  ssr: false,
  loading: () => <p>Loading Map...</p>,
});

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-between">
      <div className="h-screen w-full">
        <RainMap />
      </div>
    </main>
  );
}
