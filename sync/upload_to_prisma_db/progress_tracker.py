#!/usr/bin/env python3
"""
Progress tracker for WeChat chat data database upload
"""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class ProgressTracker:
    """Track database upload progress to avoid duplicate imports"""

    def __init__(self, progress_file: str = "chat_db_upload_progress.json"):
        self.progress_file = Path(progress_file)
        self.progress_data = self._load_progress()

    def _load_progress(self) -> Dict:
        """Load progress data from file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"⚠️  Warning: Could not load progress file: {e}")
                return {}
        return {}

    def _save_progress(self):
        """Save progress data to file"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            print(f"❌ Error saving progress file: {e}")

    def _get_file_metadata(self, file_path: Path) -> Dict:
        """Get file metadata for change detection"""
        try:
            stat = file_path.stat()
            return {
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "mtime_str": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        except Exception as e:
            print(f"⚠️  Warning: Could not get metadata for {file_path}: {e}")
            return {}

    def _calculate_file_hash(self, file_path: Path, chunk_size: int = 8192) -> str:
        """Calculate MD5 hash of file content (for small files only)"""
        if file_path.stat().st_size > 10 * 1024 * 1024:  # Skip hash for files > 10MB
            return ""

        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
            return ""

    def is_file_processed(self, file_path: Path) -> bool:
        """Check if file has been successfully processed"""
        file_key = str(file_path.absolute())

        if file_key not in self.progress_data:
            return False

        record = self.progress_data[file_key]

        # Check if processing was successful
        if record.get("status") != "success":
            return False

        # Check if file has changed since last processing
        current_metadata = self._get_file_metadata(file_path)
        stored_metadata = record.get("file_metadata", {})

        # Compare file size and modification time
        if current_metadata.get("size") != stored_metadata.get("size") or current_metadata.get(
            "mtime"
        ) != stored_metadata.get("mtime"):
            print(f"📝 File {file_path.name} has changed since last processing")
            return False

        # For small files, also check content hash
        if current_metadata.get("size", 0) <= 10 * 1024 * 1024:  # <= 10MB
            current_hash = self._calculate_file_hash(file_path)
            stored_hash = record.get("file_hash", "")
            if current_hash and stored_hash and current_hash != stored_hash:
                print(f"📝 File content of {file_path.name} has changed")
                return False

        return True

    def get_processing_info(self, file_path: Path) -> Optional[Dict]:
        """Get processing information for a file"""
        file_key = str(file_path.absolute())
        return self.progress_data.get(file_key)

    def mark_file_processing_start(self, file_path: Path):
        """Mark that file processing has started"""
        file_key = str(file_path.absolute())

        self.progress_data[file_key] = {
            "status": "processing",
            "start_time": datetime.now().isoformat(),
            "file_path": str(file_path),
            "file_metadata": self._get_file_metadata(file_path),
            "file_hash": self._calculate_file_hash(file_path),
        }

        self._save_progress()

    def mark_file_processing_success(self, file_path: Path, stats: Dict[str, Any]):
        """Mark that file processing completed successfully"""
        file_key = str(file_path.absolute())

        if file_key in self.progress_data:
            self.progress_data[file_key].update(
                {"status": "success", "end_time": datetime.now().isoformat(), "processing_stats": stats}
            )
        else:
            # If record doesn't exist, create it
            self.progress_data[file_key] = {
                "status": "success",
                "start_time": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "file_path": str(file_path),
                "file_metadata": self._get_file_metadata(file_path),
                "file_hash": self._calculate_file_hash(file_path),
                "processing_stats": stats,
            }

        self._save_progress()

    def mark_file_processing_failed(self, file_path: Path, error: str):
        """Mark that file processing failed"""
        file_key = str(file_path.absolute())

        if file_key in self.progress_data:
            self.progress_data[file_key].update(
                {"status": "failed", "end_time": datetime.now().isoformat(), "error": error}
            )
        else:
            # If record doesn't exist, create it
            self.progress_data[file_key] = {
                "status": "failed",
                "start_time": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
                "file_path": str(file_path),
                "file_metadata": self._get_file_metadata(file_path),
                "file_hash": self._calculate_file_hash(file_path),
                "error": error,
            }

        self._save_progress()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of processing progress"""
        total_files = len(self.progress_data)
        success_files = sum(1 for record in self.progress_data.values() if record.get("status") == "success")
        failed_files = sum(1 for record in self.progress_data.values() if record.get("status") == "failed")
        processing_files = sum(1 for record in self.progress_data.values() if record.get("status") == "processing")

        total_messages = sum(
            record.get("processing_stats", {}).get("messages_count", 0)
            for record in self.progress_data.values()
            if record.get("status") == "success"
        )

        total_chat_rooms = sum(
            record.get("processing_stats", {}).get("chat_rooms_count", 0)
            for record in self.progress_data.values()
            if record.get("status") == "success"
        )

        total_users = sum(
            record.get("processing_stats", {}).get("users_count", 0)
            for record in self.progress_data.values()
            if record.get("status") == "success"
        )

        return {
            "total_files": total_files,
            "success_files": success_files,
            "failed_files": failed_files,
            "processing_files": processing_files,
            "success_rate": f"{(success_files/total_files*100):.1f}%" if total_files > 0 else "0%",
            "total_messages": total_messages,
            "total_chat_rooms": total_chat_rooms,
            "total_users": total_users,
        }

    def print_summary(self):
        """Print a summary of processing progress"""
        summary = self.get_summary()

        print(f"\n📊 Database Upload Progress Summary:")
        print(f"   Total files: {summary['total_files']}")
        print(f"   ✅ Success: {summary['success_files']}")
        print(f"   ❌ Failed: {summary['failed_files']}")
        print(f"   🔄 Processing: {summary['processing_files']}")
        print(f"   Success rate: {summary['success_rate']}")
        print(f"\n📈 Processed Data:")
        print(f"   Messages: {summary['total_messages']:,}")
        print(f"   Chat rooms: {summary['total_chat_rooms']:,}")
        print(f"   Users: {summary['total_users']:,}")

    def get_failed_files(self) -> List[Dict]:
        """Get list of failed files with error details"""
        failed_files = []
        for file_path, record in self.progress_data.items():
            if record.get("status") == "failed":
                failed_files.append(
                    {
                        "file_path": file_path,
                        "error": record.get("error", "Unknown error"),
                        "time": record.get("end_time", "Unknown time"),
                    }
                )
        return failed_files

    def reset_file_status(self, file_path: Path):
        """Reset processing status for a specific file (to retry processing)"""
        file_key = str(file_path.absolute())
        if file_key in self.progress_data:
            del self.progress_data[file_key]
            self._save_progress()
            print(f"🔄 Reset processing status for {file_path.name}")

    def reset_failed_files(self):
        """Reset all failed files to allow retry"""
        failed_count = 0
        for file_path, record in list(self.progress_data.items()):
            if record.get("status") == "failed":
                del self.progress_data[file_path]
                failed_count += 1

        if failed_count > 0:
            self._save_progress()
            print(f"🔄 Reset {failed_count} failed files for retry")
        else:
            print("ℹ️  No failed files to reset")
