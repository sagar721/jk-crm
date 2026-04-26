const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const dist = path.join(root, "dist");
const files = ["index.html", "app.js", "styles.css", "README.md"];

function copyFile(name) {
  fs.copyFileSync(path.join(root, name), path.join(dist, name));
}

function copyDir(source, target) {
  if (!fs.existsSync(source)) return;
  fs.mkdirSync(target, { recursive: true });
  for (const entry of fs.readdirSync(source, { withFileTypes: true })) {
    const from = path.join(source, entry.name);
    const to = path.join(target, entry.name);
    if (entry.isDirectory()) copyDir(from, to);
    else fs.copyFileSync(from, to);
  }
}

fs.rmSync(dist, { recursive: true, force: true });
fs.mkdirSync(dist, { recursive: true });

execFileSync(process.execPath, ["--check", path.join(root, "app.js")], { stdio: "inherit" });
execFileSync("python3", ["-m", "py_compile", path.join(root, "server.py")], { stdio: "inherit" });

for (const file of files) {
  if (file === "app.js") {
    let content = fs.readFileSync(path.join(root, file), "utf8");
    let apiUrl = process.env.VITE_API_BASE_URL;
    if (!apiUrl) {
      if (process.env.VERCEL) {
        console.error("FATAL: VITE_API_BASE_URL is not set in Vercel environment variables.");
        process.exit(1);
      }
      apiUrl = "http://127.0.0.1:8765";
    }
    content = content.replace("VITE_API_BASE_URL", apiUrl);
    fs.writeFileSync(path.join(dist, file), content);
  } else {
    copyFile(file);
  }
}
copyDir(path.join(root, "assets"), path.join(dist, "assets"));

const meta = {
  builtAt: new Date().toISOString(),
  files: fs.readdirSync(dist).sort()
};
fs.writeFileSync(path.join(dist, "build.json"), JSON.stringify(meta, null, 2));

console.log(`Built CRM into ${dist}`);
