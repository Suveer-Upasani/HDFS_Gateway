"""Camera stream interface for cross-platform video capture (Windows, macOS, Linux)."""

from abc import ABC, abstractmethod
from typing import Any


class BaseCameraStream(ABC):
    """Abstract interface for video frame acquisition from camera hardware or stream."""

    @abstractmethod
    def open(self) -> bool:
        """Initialize and open the video capture device or network stream."""

    @abstractmethod
    def read(self) -> tuple[bool, Any | None]:
        """Read a single frame from the camera stream.

        Returns:
            Tuple of (success: bool, frame: Optional[ndarray]).
        """

    @abstractmethod
    def release(self) -> None:
        """Release camera hardware or stream resources."""

    @abstractmethod
    def is_opened(self) -> bool:
        """Check if camera device is active and open."""


class OpenCVCameraStream(BaseCameraStream):
    """Standard camera capture implementation using OpenCV.

    Provides cross-platform support across Windows (MSMF/DSHOW), macOS (AVFoundation),
    and Linux (V4L2) without OS-specific hardcoding.
    """

    def __init__(self, source: str | int = 0) -> None:
        if isinstance(source, int):
            self.source: int | str = source
        else:
            self.source = int(source) if str(source).strip().isdigit() else str(source).strip()
        self._cap: Any | None = None

    def open(self) -> bool:
        """Open OpenCV video capture device or stream.

        Returns:
            True if capture device opened successfully, False otherwise.

        Raises:
            ImportError: If opencv-python is not installed.
            RuntimeError: If device initialization encounters an unexpected error.
        """
        try:
            import cv2

            self._cap = cv2.VideoCapture(self.source)
            return bool(self._cap.isOpened())
        except ImportError as err:
            raise ImportError(
                "OpenCV (opencv-python) is required for live camera capture. "
                "Install it with `pip install opencv-python`."
            ) from err
        except Exception as err:
            raise RuntimeError(
                f"Failed to initialize video capture device for source '{self.source}': {err}"
            ) from err

    def read(self) -> tuple[bool, Any | None]:
        """Read the next frame from the camera stream."""
        if self._cap is None or not self._cap.isOpened():
            return False, None
        return self._cap.read()

    def release(self) -> None:
        """Cleanly release OpenCV VideoCapture resources."""
        if self._cap is not None:
            self._cap.release()
            self._cap = None

    def is_opened(self) -> bool:
        """Check if camera device is currently active and opened."""
        return self._cap is not None and bool(self._cap.isOpened())
