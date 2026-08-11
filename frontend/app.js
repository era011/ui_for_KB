(() => {
  "use strict";

  const API = "/api";
  const SUPPORTED_EXTENSIONS = [".txt", ".pdf", ".docx"];
  const DOC_ICONS = { ".pdf": "📕", ".docx": "📘", ".txt": "📄" };

  const el = {
    searchInput: document.getElementById("search-input"),
    docList: document.getElementById("doc-list"),
    docCount: document.getElementById("doc-count"),
    viewerEmpty: document.getElementById("viewer-empty"),
    viewerContent: document.getElementById("viewer-content"),
    viewerTitle: document.getElementById("viewer-title"),
    viewerClose: document.getElementById("viewer-close"),
    chunksList: document.getElementById("chunks-list"),
    uploadBtn: document.getElementById("upload-btn"),
    uploadModal: document.getElementById("upload-modal"),
    uploadModalClose: document.getElementById("upload-modal-close"),
    dropzone: document.getElementById("dropzone"),
    fileInput: document.getElementById("file-input"),
    uploadResults: document.getElementById("upload-results"),
    toastContainer: document.getElementById("toast-container"),
    logsBtn: document.getElementById("logs-btn"),
    logsModal: document.getElementById("logs-modal"),
    logsModalClose: document.getElementById("logs-modal-close"),
    logsRefresh: document.getElementById("logs-refresh"),
    logsList: document.getElementById("logs-list"),
  };

  const state = {
    documents: [],
    activeId: null,
    search: "",
  };

  // ---------- helpers ----------

  function escapeHtml(str) {
    const d = document.createElement("div");
    d.textContent = str ?? "";
    return d.innerHTML;
  }

  function formatDate(value) {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return value;
    return d.toLocaleString("ru-RU", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function showToast(message, type = "default") {
    const toast = document.createElement("div");
    toast.className = `toast ${type}`;
    toast.textContent = message;
    el.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.transition = "opacity 0.25s ease";
      toast.style.opacity = "0";
      setTimeout(() => toast.remove(), 250);
    }, 3500);
  }

  async function apiFetch(path, options) {
    const res = await fetch(`${API}${path}`, options);
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const body = await res.json();
        detail = body.detail || detail;
      } catch (_) {
        /* ignore */
      }
      throw new Error(detail);
    }
    return res.status === 204 ? null : res.json();
  }

  // ---------- documents list ----------

  async function loadDocuments() {
    el.docList.innerHTML = `<div class="empty-state">Загрузка…</div>`;
    try {
      const query = state.search ? `?search=${encodeURIComponent(state.search)}` : "";
      const docs = await apiFetch(`/documents${query}`);
      state.documents = docs;
      renderDocuments();
    } catch (e) {
      el.docList.innerHTML = `<div class="empty-state error">Не удалось загрузить документы: ${escapeHtml(e.message)}</div>`;
      el.docCount.textContent = "0";
    }
  }

  function iconForName(name) {
    const lower = (name || "").toLowerCase();
    const ext = SUPPORTED_EXTENSIONS.find((e) => lower.endsWith(e));
    return DOC_ICONS[ext] || "📄";
  }

  function renderDocuments() {
    el.docCount.textContent = String(state.documents.length);

    if (!state.documents.length) {
      el.docList.innerHTML = `<div class="empty-state">Ничего не найдено</div>`;
      return;
    }

    el.docList.innerHTML = "";
    for (const doc of state.documents) {
      const card = document.createElement("div");
      card.className = "doc-card" + (doc.id_doc === state.activeId ? " active" : "");
      card.dataset.id = doc.id_doc;
      card.innerHTML = `
        <div class="doc-icon">${iconForName(doc.name)}</div>
        <div class="doc-info">
          <div class="doc-name" title="${escapeHtml(doc.name)}">${escapeHtml(doc.name)}</div>
          <div class="doc-meta">
            <span>${formatDate(doc.added_date)}</span>
            <span class="dot">·</span>
            <span>${doc.chunks_count} чанков</span>
          </div>
        </div>
        <button class="btn-icon danger" type="button" title="Удалить" aria-label="Удалить документ">🗑</button>
      `;

      card.addEventListener("click", (ev) => {
        if (ev.target.closest("button")) return;
        openDocument(doc);
      });

      card.querySelector("button").addEventListener("click", (ev) => {
        ev.stopPropagation();
        deleteDocument(doc);
      });

      el.docList.appendChild(card);
    }
  }

  // ---------- viewer ----------

  async function openDocument(doc) {
    state.activeId = doc.id_doc;
    renderDocuments();

    el.viewerEmpty.classList.add("hidden");
    el.viewerContent.classList.remove("hidden");
    el.viewerTitle.textContent = doc.name;
    el.chunksList.innerHTML = `<div class="empty-state"><span class="spinner"></span></div>`;

    try {
      const data = await apiFetch(`/documents/${encodeURIComponent(doc.id_doc)}/chunks`);
      renderChunks(data.chunks);
    } catch (e) {
      el.chunksList.innerHTML = `<div class="empty-state error">Не удалось загрузить содержимое: ${escapeHtml(e.message)}</div>`;
    }
  }

  function renderChunks(chunks) {
    if (!chunks || !chunks.length) {
      el.chunksList.innerHTML = `<div class="empty-state">Чанки не найдены</div>`;
      return;
    }
    el.chunksList.innerHTML = "";
    chunks.forEach((text, idx) => {
      const isMeta = idx === chunks.length - 1 && /^\{'chunk_index'/.test(text);
      const box = document.createElement("div");
      box.className = "chunk" + (isMeta ? " chunk-meta" : "");
      box.innerHTML = `
        <div class="chunk-index">${isMeta ? "Метаданные" : `Чанк ${idx + 1}`}</div>
        <div class="chunk-text"></div>
      `;
      box.querySelector(".chunk-text").textContent = text;
      el.chunksList.appendChild(box);
    });
  }

  function closeViewer() {
    state.activeId = null;
    el.viewerContent.classList.add("hidden");
    el.viewerEmpty.classList.remove("hidden");
    renderDocuments();
  }

  // ---------- delete ----------

  async function deleteDocument(doc) {
    if (!confirm(`Удалить документ «${doc.name}»? Это действие необратимо.`)) return;
    try {
      const query = `?name=${encodeURIComponent(doc.name)}`;
      await apiFetch(`/documents/${encodeURIComponent(doc.id_doc)}${query}`, { method: "DELETE" });
      showToast(`Документ «${doc.name}» удалён`, "success");
      if (state.activeId === doc.id_doc) closeViewer();
      loadDocuments();
    } catch (e) {
      showToast(`Ошибка при удалении: ${e.message}`, "error");
    }
  }

  // ---------- upload ----------

  function openUploadModal() {
    el.uploadResults.innerHTML = "";
    el.uploadModal.classList.remove("hidden");
  }

  function closeUploadModal() {
    el.uploadModal.classList.add("hidden");
    el.fileInput.value = "";
  }

  async function uploadFiles(fileList) {
    const files = Array.from(fileList).filter((f) =>
      SUPPORTED_EXTENSIONS.some((ext) => f.name.toLowerCase().endsWith(ext))
    );
    if (!files.length) {
      showToast(`Выберите файлы: ${SUPPORTED_EXTENSIONS.join(", ")}`, "error");
      return;
    }

    el.uploadResults.innerHTML = "";
    const rows = new Map();
    for (const f of files) {
      const row = document.createElement("div");
      row.className = "upload-result pending";
      row.innerHTML = `<span class="spinner"></span><div><span class="name">${escapeHtml(f.name)}</span><span class="msg">Загрузка…</span></div>`;
      el.uploadResults.appendChild(row);
      rows.set(f.name, row);
    }

    const formData = new FormData();
    for (const f of files) formData.append("files", f, f.name);

    try {
      const results = await apiFetch(`/documents/upload`, { method: "POST", body: formData });
      let okCount = 0;
      for (const r of results) {
        const row = rows.get(r.name) || document.createElement("div");
        row.className = "upload-result " + (r.ok ? "ok" : "fail");
        row.innerHTML = `<div><span class="name">${escapeHtml(r.name)}</span><span class="msg">${escapeHtml(r.message)}</span></div>`;
        if (r.ok) okCount++;
      }
      if (okCount) {
        showToast(`Добавлено документов: ${okCount}`, "success");
        loadDocuments();
      }
    } catch (e) {
      showToast(`Ошибка загрузки: ${e.message}`, "error");
      el.uploadResults.innerHTML = `<div class="upload-result fail"><div><span class="msg">${escapeHtml(e.message)}</span></div></div>`;
    }
  }

  // ---------- logs ----------

  const ACTION_LABELS = { upload: "Загрузка", delete: "Удаление" };

  function openLogsModal() {
    el.logsModal.classList.remove("hidden");
    loadLogs();
  }

  function closeLogsModal() {
    el.logsModal.classList.add("hidden");
  }

  async function loadLogs() {
    el.logsList.innerHTML = `<div class="empty-state"><span class="spinner"></span></div>`;
    try {
      const logs = await apiFetch(`/logs`);
      renderLogs(logs);
    } catch (e) {
      el.logsList.innerHTML = `<div class="empty-state error">Не удалось загрузить логи: ${escapeHtml(e.message)}</div>`;
    }
  }

  function renderLogs(logs) {
    if (!logs || !logs.length) {
      el.logsList.innerHTML = `<div class="empty-state">Действий пока не было</div>`;
      return;
    }
    el.logsList.innerHTML = "";
    for (const log of logs) {
      const row = document.createElement("div");
      row.className = "log-row " + (log.status === "success" ? "ok" : "fail");
      const actionLabel = ACTION_LABELS[log.action] || log.action;
      row.innerHTML = `
        <div class="log-icon">${log.action === "upload" ? "⬆️" : "🗑"}</div>
        <div class="log-body">
          <div class="log-title">
            <span class="log-action">${escapeHtml(actionLabel)}</span>
            <span class="log-doc-name">${escapeHtml(log.doc_name || log.doc_id || "—")}</span>
          </div>
          ${log.message ? `<div class="log-message">${escapeHtml(log.message)}</div>` : ""}
        </div>
        <div class="log-time">${log.created_at ? formatDate(log.created_at) : ""}</div>
      `;
      el.logsList.appendChild(row);
    }
  }

  // ---------- search ----------

  let searchTimer = null;
  el.searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = el.searchInput.value.trim();
      loadDocuments();
    }, 300);
  });

  // ---------- wiring ----------

  el.viewerClose.addEventListener("click", closeViewer);
  el.uploadBtn.addEventListener("click", openUploadModal);
  el.uploadModalClose.addEventListener("click", closeUploadModal);
  el.uploadModal.addEventListener("click", (ev) => {
    if (ev.target === el.uploadModal) closeUploadModal();
  });
  el.logsBtn.addEventListener("click", openLogsModal);
  el.logsModalClose.addEventListener("click", closeLogsModal);
  el.logsRefresh.addEventListener("click", loadLogs);
  el.logsModal.addEventListener("click", (ev) => {
    if (ev.target === el.logsModal) closeLogsModal();
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key !== "Escape") return;
    if (!el.uploadModal.classList.contains("hidden")) closeUploadModal();
    if (!el.logsModal.classList.contains("hidden")) closeLogsModal();
  });

  el.fileInput.addEventListener("change", () => {
    if (el.fileInput.files.length) uploadFiles(el.fileInput.files);
  });

  ["dragenter", "dragover"].forEach((evt) =>
    el.dropzone.addEventListener(evt, (ev) => {
      ev.preventDefault();
      el.dropzone.classList.add("dragover");
    })
  );
  ["dragleave", "drop"].forEach((evt) =>
    el.dropzone.addEventListener(evt, (ev) => {
      ev.preventDefault();
      el.dropzone.classList.remove("dragover");
    })
  );
  el.dropzone.addEventListener("drop", (ev) => {
    const files = ev.dataTransfer?.files;
    if (files && files.length) uploadFiles(files);
  });

  // ---------- init ----------

  loadDocuments();
})();
