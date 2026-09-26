/**
 * HDFS Gateway - Client-side Application Controller
 * Vanilla JavaScript implementation
 */

// Toast Notifications System
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  
  let iconName = 'info';
  if (type === 'success') iconName = 'check-circle';
  if (type === 'error') iconName = 'alert-circle';
  if (type === 'warning') iconName = 'alert-triangle';

  toast.innerHTML = `
    <i data-lucide="${iconName}" style="width: 18px; height: 18px; flex-shrink: 0;"></i>
    <div style="flex: 1; word-break: break-word;">${escapeHtml(message)}</div>
  `;

  container.appendChild(toast);
  if (window.lucide) {
    window.lucide.createIcons({ root: toast });
  }

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    toast.style.transition = 'all 0.2s ease';
    setTimeout(() => toast.remove(), 200);
  }, 4000);
}

// Utility to escape HTML strings safely
function escapeHtml(text) {
  if (!text) return '';
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Global Delete Modal State
let pendingDeleteFilename = null;

function openDeleteModal(filename) {
  pendingDeleteFilename = filename;
  const modal = document.getElementById('delete-modal');
  const filenameEl = document.getElementById('delete-modal-filename');
  if (filenameEl) {
    filenameEl.textContent = filename;
  }
  if (modal) {
    modal.classList.add('active');
  }
}

function closeDeleteModal() {
  pendingDeleteFilename = null;
  const modal = document.getElementById('delete-modal');
  if (modal) {
    modal.classList.remove('active');
  }
}

async function confirmDelete() {
  if (!pendingDeleteFilename) return;

  const confirmBtn = document.getElementById('delete-modal-confirm');
  if (confirmBtn) {
    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Deleting...';
  }

  try {
    const response = await fetch(`/api/files/${encodeURIComponent(pendingDeleteFilename)}`, {
      method: 'DELETE',
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || data.error || 'Failed to delete file from HDFS');
    }

    showToast(data.message || `File '${pendingDeleteFilename}' was deleted.`, 'success');
    closeDeleteModal();

    // Remove row from table if on files/dashboard page
    const row = document.querySelector(`tr[data-filename="${CSS.escape(pendingDeleteFilename)}"]`);
    if (row) {
      row.remove();
      // Check if table is now empty
      const tbody = document.querySelector('.data-table tbody');
      if (tbody && tbody.children.length === 0) {
        window.location.reload();
      }
    } else {
      setTimeout(() => window.location.reload(), 500);
    }
  } catch (error) {
    showToast(error.message, 'error');
  } finally {
    if (confirmBtn) {
      confirmBtn.disabled = false;
      confirmBtn.textContent = 'Delete';
    }
  }
}

// Search Filter for Files Tables
function setupTableSearch() {
  const searchInput = document.getElementById('file-search-input');
  if (!searchInput) return;

  searchInput.addEventListener('input', (e) => {
    const query = e.target.value.toLowerCase().trim();
    const rows = document.querySelectorAll('.data-table tbody tr');

    let visibleCount = 0;
    rows.forEach((row) => {
      const filename = row.getAttribute('data-filename') || '';
      if (filename.toLowerCase().includes(query)) {
        row.style.display = '';
        visibleCount++;
      } else {
        row.style.display = 'none';
      }
    });

    // Toggle search empty message
    const noResultsRow = document.getElementById('no-search-results');
    if (noResultsRow) {
      noResultsRow.style.display = (visibleCount === 0 && rows.length > 0) ? '' : 'none';
    }
  });
}

// Upload Page Drag-and-Drop & Submission
function setupUpload() {
  const dropzone = document.getElementById('upload-dropzone');
  const fileInput = document.getElementById('file-input');
  const uploadForm = document.getElementById('upload-form');
  const previewArea = document.getElementById('file-preview-area');
  const previewName = document.getElementById('file-preview-name');
  const previewSize = document.getElementById('file-preview-size');
  const removeFileBtn = document.getElementById('remove-file-btn');
  const submitBtn = document.getElementById('upload-submit-btn');
  const resultAlert = document.getElementById('upload-result-alert');

  if (!dropzone || !fileInput) return;

  let selectedFile = null;

  function updateSelectedFile(file) {
    selectedFile = file;
    if (file) {
      if (previewName) previewName.textContent = file.name;
      if (previewSize) previewSize.textContent = formatBytes(file.size);
      if (previewArea) previewArea.style.display = 'flex';
      if (submitBtn) submitBtn.disabled = false;
      if (resultAlert) resultAlert.style.display = 'none';
    } else {
      fileInput.value = '';
      if (previewArea) previewArea.style.display = 'none';
      if (submitBtn) submitBtn.disabled = true;
    }
    if (window.lucide) window.lucide.createIcons();
  }

  // Click to open file dialog
  dropzone.addEventListener('click', (e) => {
    if (e.target.closest('#remove-file-btn') || e.target.closest('#file-preview-area')) return;
    fileInput.click();
  });

  fileInput.addEventListener('change', () => {
    if (fileInput.files && fileInput.files[0]) {
      updateSelectedFile(fileInput.files[0]);
    }
  });

  if (removeFileBtn) {
    removeFileBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      updateSelectedFile(null);
    });
  }

  // Drag and drop event handlers
  ['dragenter', 'dragover'].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add('dragover');
    });
  });

  ['dragleave', 'drop'].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('dragover');
    });
  });

  dropzone.addEventListener('drop', (e) => {
    if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]) {
      updateSelectedFile(e.dataTransfer.files[0]);
    }
  });

  // Form submission via async POST
  if (uploadForm) {
    uploadForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (!selectedFile) {
        showToast('Please select a file to upload.', 'warning');
        return;
      }

      if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = `
          <i data-lucide="loader-2" class="spin" style="width: 16px; height: 16px;"></i>
          Uploading...
        `;
        if (window.lucide) window.lucide.createIcons();
      }

      const formData = new FormData();
      formData.append('file', selectedFile);

      try {
        const response = await fetch('/api/upload', {
          method: 'POST',
          body: formData,
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.detail || data.error || 'Upload failed');
        }

        showToast(`'${data.filename}' successfully uploaded to HDFS!`, 'success');

        if (resultAlert) {
          resultAlert.className = 'alert alert-success';
          resultAlert.style.display = 'block';
          resultAlert.innerHTML = `
            <div style="display: flex; gap: 0.75rem; align-items: flex-start;">
              <i data-lucide="check-circle-2" style="width: 20px; height: 20px; color: var(--success); flex-shrink: 0;"></i>
              <div>
                <strong>✓ Upload Successful</strong>
                <div style="margin-top: 0.35rem; font-size: 0.85rem;">
                  <div><strong>Filename:</strong> ${escapeHtml(data.filename)}</div>
                  <div><strong>Size:</strong> ${escapeHtml(data.size_formatted)}</div>
                  <div><strong>HDFS Path:</strong> <code style="font-family: var(--font-mono); color: #7ee787;">${escapeHtml(data.path)}</code></div>
                </div>
                <div style="margin-top: 0.75rem;">
                  <a href="/files" class="btn btn-sm btn-secondary">View in Files Manager →</a>
                </div>
              </div>
            </div>
          `;
          if (window.lucide) window.lucide.createIcons();
        }

        updateSelectedFile(null);

      } catch (error) {
        showToast(error.message, 'error');
        if (resultAlert) {
          resultAlert.className = 'alert alert-danger';
          resultAlert.style.display = 'block';
          resultAlert.innerHTML = `
            <div style="display: flex; gap: 0.75rem; align-items: flex-start;">
              <i data-lucide="alert-circle" style="width: 20px; height: 20px; color: var(--danger); flex-shrink: 0;"></i>
              <div>
                <strong>Upload Failed</strong>
                <div style="margin-top: 0.25rem; font-size: 0.85rem;">${escapeHtml(error.message)}</div>
              </div>
            </div>
          `;
          if (window.lucide) window.lucide.createIcons();
        }
      } finally {
        if (submitBtn) {
          submitBtn.disabled = selectedFile === null;
          submitBtn.innerHTML = `
            <i data-lucide="upload-cloud" style="width: 16px; height: 16px;"></i>
            Upload to HDFS
          `;
          if (window.lucide) window.lucide.createIcons();
        }
      }
    });
  }
}

// Client-side byte formatting helper
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Document Ready Initialization
document.addEventListener('DOMContentLoaded', () => {
  // Initialize Lucide icons
  if (window.lucide) {
    window.lucide.createIcons();
  }

  // Setup sub-modules
  setupTableSearch();
  setupUpload();

  // Attach modal close on backdrop click
  const modalBackdrop = document.getElementById('delete-modal');
  if (modalBackdrop) {
    modalBackdrop.addEventListener('click', (e) => {
      if (e.target === modalBackdrop) {
        closeDeleteModal();
      }
    });
  }

  // Keyboard shortcut Esc to close modal
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeDeleteModal();
    }
  });
});
