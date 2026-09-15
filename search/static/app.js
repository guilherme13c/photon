const form = document.querySelector("#search-form");
const input = document.querySelector("#query");
const limit = document.querySelector("#limit");
const results = document.querySelector("#results");
const status = document.querySelector("#status");
const loadMore = document.querySelector("#load-more");
let nextCursor = "";
let activeQuery = "";

function setStatus(message, isError = false) {
  status.textContent = message;
  status.classList.toggle("error", isError);
}

function text(value, fallback = "") { return value == null || value === "" ? fallback : String(value); }

function renderResult(result, index) {
  const article = document.createElement("article");
  article.className = "result";
  article.style.animationDelay = `${Math.min(index, 8) * 35}ms`;
  const meta = document.createElement("div");
  meta.className = "result-meta";
  meta.innerHTML = `<span>CHUNK ${Number(result.chunk_index ?? 0) + 1}</span><span class="score">Hybrid relevance ${(Number(result.score || 0)).toFixed(3)}</span>`;
  const heading = document.createElement("h3");
  const title = text(result.title, text(result.url, "Untitled document"));
  if (result.url) {
    const link = document.createElement("a");
    link.href = result.url; link.target = "_blank"; link.rel = "noopener noreferrer"; link.textContent = title;
    heading.append(link);
  } else heading.textContent = title;
  const excerpt = document.createElement("p"); excerpt.textContent = text(result.text, "No excerpt available.");
  article.append(meta, heading, excerpt);
  return article;
}

async function search({ append = false } = {}) {
  const query = input.value.trim();
  if (!query) { setStatus("Enter a question to search.", true); input.focus(); return; }
  if (!append) { activeQuery = query; nextCursor = ""; results.replaceChildren(); }
  const params = new URLSearchParams({ q: activeQuery, limit: limit.value });
  if (append && nextCursor) params.set("cursor", nextCursor);
  form.querySelector("button").disabled = true; loadMore.disabled = true;
  setStatus(append ? "Finding more passages…" : "Searching the index…");
  try {
    const response = await fetch(`/v1/search?${params}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Search failed.");
    payload.results.forEach((result, index) => results.append(renderResult(result, index)));
    nextCursor = payload.next_cursor || "";
    loadMore.hidden = !nextCursor;
    setStatus(`${results.children.length} result${results.children.length === 1 ? "" : "s"} shown.`);
    if (!payload.results.length && !append) results.innerHTML = '<div class="empty">No matching passages yet.</div>';
  } catch (error) { setStatus(error.message || "Could not reach the search service.", true); loadMore.hidden = true; }
  finally { form.querySelector("button").disabled = false; loadMore.disabled = false; }
}

form.addEventListener("submit", (event) => { event.preventDefault(); search(); });
loadMore.addEventListener("click", () => search({ append: true }));
