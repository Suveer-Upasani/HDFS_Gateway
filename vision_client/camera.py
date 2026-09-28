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
        self.source: int | str = int(source) if str(source).isdigit() else source
        self._cap: Any | None = None

    def open(self) -> bool:
        try:
            import cv2
            self._cap = cv2.VideoCapture(self.source)
            return bool(self._cap.isOpened())
        except ImportError as err:
            raise ImportError(
                "OpenCV (opencv-python) is required for live camera capture. "
                "Install it with `pip install opencv-python`."
            ) from err

    def read(self) -> tuple[bool, Any | None]:
        if self._cap is None or not self._cap.isOpened():
            return False, None
        return self._cap.read()

    def release(self) -> None:
        if self._cap is not None:
            self._cap.release()
            self._cap = None

    def is_opened(self) -> bool:
        return self._cap is not None and bool(self._cap.isOpened())
