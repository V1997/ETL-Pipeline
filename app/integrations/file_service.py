    
    
import os
import shutil
import logging
import glob
import json
import csv
import pandas as pd
from typing import Dict, Any, List, Optional, Union, BinaryIO
from datetime import datetime
from pathlib import Path
import hashlib
import io
import zipfile
import tempfile

# Set up logging
logger = logging.getLogger(__name__)

class FileConfig:
    """File system configuration settings from environment variables."""
    
    BASE_DIR = os.getenv("FILE_BASE_DIR", os.path.join(os.getcwd(), "data"))
    INPUT_DIR = os.getenv("FILE_INPUT_DIR", os.path.join(BASE_DIR, "input"))
    PROCESSED_DIR = os.getenv("FILE_PROCESSED_DIR", os.path.join(BASE_DIR, "processed"))
    ERROR_DIR = os.getenv("FILE_ERROR_DIR", os.path.join(BASE_DIR, "error"))
    ARCHIVE_DIR = os.getenv("FILE_ARCHIVE_DIR", os.path.join(BASE_DIR, "archive"))
    OUTPUT_DIR = os.getenv("FILE_OUTPUT_DIR", os.path.join(BASE_DIR, "output"))
    
    # Make sure directories exist
    for directory in [BASE_DIR, INPUT_DIR, PROCESSED_DIR, ERROR_DIR, ARCHIVE_DIR, OUTPUT_DIR]:
        os.makedirs(directory, exist_ok=True)

class FileService:
    """
    File system operations service for ETL pipeline.
    
    This class handles:
    - File reading/writing in various formats (CSV, JSON, Excel)
    - File validation (checksums, size limits)
    - File movement between directories
    - Archiving and cleanup operations
    """
    
    def __init__(self, config: Dict[str, str] = None):
        """
        Initialize the file service.
        
        Args:
            config: Optional configuration overrides
        """
        self.config = {
            'base_dir': FileConfig.BASE_DIR,
            'input_dir': FileConfig.INPUT_DIR,
            'processed_dir': FileConfig.PROCESSED_DIR,
            'error_dir': FileConfig.ERROR_DIR,
            'archive_dir': FileConfig.ARCHIVE_DIR,
            'output_dir': FileConfig.OUTPUT_DIR
        }
        
        if config:
            self.config.update(config)
        
        # Ensure directories exist
        for directory in self.config.values():
            os.makedirs(directory, exist_ok=True)
        
        logger.info(f"File service initialized with base directory: {self.config['base_dir']}")
    
    def list_input_files(self, pattern: str = "*") -> List[str]:
        """
        List files in the input directory matching a pattern.
        
        Args:
            pattern: Glob pattern to match filenames
            
        Returns:
            List[str]: List of matching file paths
        """
        search_pattern = os.path.join(self.config['input_dir'], pattern)
        return glob.glob(search_pattern)
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a file into a pandas DataFrame.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            pd.DataFrame: Data from the file
            
        Raises:
            ValueError: For unsupported file types or parsing errors
        """
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                return pd.read_csv(file_path)
            elif file_ext in ['.xls', '.xlsx']:
                return pd.read_excel(file_path)
            elif file_ext == '.json':
                return pd.read_json(file_path)
            elif file_ext == '.parquet':
                return pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise ValueError(f"Failed to read file {file_path}: {str(e)}")
    
    def write_file(
        self, data: Union[pd.DataFrame, Dict, List], 
        file_name: str, 
        file_format: str = 'csv'
    ) -> str:
        """
        Write data to a file in the output directory.
        
        Args:
            data: Data to write (DataFrame, dict, or list)
            file_name: Output file name
            file_format: Output file format ('csv', 'excel', 'json', 'parquet')
            
        Returns:
            str: Path to the written file
        """
        # Ensure the data is a DataFrame
        if isinstance(data, (dict, list)):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        # Create the output file path
        file_path = os.path.join(self.config['output_dir'], file_name)
        
        # Write the data in the specified format
        try:
            if file_format.lower() == 'csv':
                df.to_csv(file_path, index=False)
            elif file_format.lower() in ['xls', 'xlsx', 'excel']:
                df.to_excel(file_path, index=False)
            elif file_format.lower() == 'json':
                df.to_json(file_path, orient='records', date_format='iso')
            elif file_format.lower() == 'parquet':
                df.to_parquet(file_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
            logger.info(f"File written successfully: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error writing file {file_path}: {str(e)}")
            raise ValueError(f"Failed to write file {file_path}: {str(e)}")
    
    def move_file(self, source_path: str, destination_dir: str) -> str:
        """
        Move a file to a destination directory.
        
        Args:
            source_path: Path to the source file
            destination_dir: Destination directory
            
        Returns:
            str: Path to the file in the new location
        """
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
            
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir, exist_ok=True)
            
        file_name = os.path.basename(source_path)
        destination_path = os.path.join(destination_dir, file_name)
        
        # If file already exists in destination, add timestamp
        if os.path.exists(destination_path):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            name, ext = os.path.splitext(file_name)
            destination_path = os.path.join(destination_dir, f"{name}_{timestamp}{ext}")
        
        try:
            shutil.move(source_path, destination_path)
            logger.info(f"Moved file: {source_path} -> {destination_path}")
            return destination_path
        except Exception as e:
            logger.error(f"Error moving file {source_path}: {str(e)}")
            raise
    
    def archive_file(self, file_path: str, delete_original: bool = False) -> str:
        """
        Archive a file to the archive directory.
        
        Args:
            file_path: Path to the file to archive
            delete_original: Whether to delete the original file
            
        Returns:
            str: Path to the archived file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        # Create timestamp-based directory structure in archive
        timestamp = datetime.now().strftime("%Y/%m/%d")
        archive_dir = os.path.join(self.config['archive_dir'], timestamp)
        os.makedirs(archive_dir, exist_ok=True)
        
        file_name = os.path.basename(file_path)
        archive_path = os.path.join(archive_dir, file_name)
        
        # If file already exists in destination, add timestamp
        if os.path.exists(archive_path):
            ts = datetime.now().strftime("%H%M%S")
            name, ext = os.path.splitext(file_name)
            archive_path = os.path.join(archive_dir, f"{name}_{ts}{ext}")
        
        try:
            if delete_original:
                shutil.move(file_path, archive_path)
                logger.info(f"Moved file to archive: {file_path} -> {archive_path}")
            else:
                shutil.copy2(file_path, archive_path)
                logger.info(f"Copied file to archive: {file_path} -> {archive_path}")
                
            return archive_path
        except Exception as e:
            logger.error(f"Error archiving file {file_path}: {str(e)}")
            raise
    
    def mark_as_error(self, file_path: str, error_info: Dict[str, Any] = None) -> str:
        """
        Move a file to the error directory and optionally save error information.
        
        Args:
            file_path: Path to the file with errors
            error_info: Optional error information to save
            
        Returns:
            str: Path to the file in the error directory
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        file_name = os.path.basename(file_path)
        error_path = os.path.join(self.config['error_dir'], file_name)
        
        # If file already exists in error dir, add timestamp
        if os.path.exists(error_path):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            name, ext = os.path.splitext(file_name)
            error_path = os.path.join(self.config['error_dir'], f"{name}_{timestamp}{ext}")
        
        try:
            # Move file to error directory
            shutil.move(file_path, error_path)
            logger.info(f"Moved file to error directory: {file_path} -> {error_path}")
            
            # Save error information if provided
            if error_info:
                error_info_path = f"{error_path}.error.json"
                with open(error_info_path, 'w') as f:
                    json.dump(error_info, f, indent=2)
                logger.info(f"Saved error information: {error_info_path}")
            
            return error_path
        except Exception as e:
            logger.error(f"Error moving file to error directory {file_path}: {str(e)}")
            raise
    
    def mark_as_processed(self, file_path: str) -> str:
        """
        Move a file to the processed directory.
        
        Args:
            file_path: Path to the processed file
            
        Returns:
            str: Path to the file in the processed directory
        """
        return self.move_file(file_path, self.config['processed_dir'])
    
    def compress_file(self, file_path: str, delete_original: bool = False) -> str:
        """
        Compress a file using zip format.
        
        Args:
            file_path: Path to the file to compress
            delete_original: Whether to delete the original file
            
        Returns:
            str: Path to the compressed file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        file_name = os.path.basename(file_path)
        dir_name = os.path.dirname(file_path)
        zip_path = os.path.join(dir_name, f"{file_name}.zip")
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, file_name)
            
            logger.info(f"Compressed file: {file_path} -> {zip_path}")
            
            if delete_original:
                os.remove(file_path)
                logger.info(f"Deleted original file: {file_path}")
            
            return zip_path
        except Exception as e:
            logger.error(f"Error compressing file {file_path}: {str(e)}")
            raise
    
    def calculate_checksum(self, file_path: str, algorithm: str = 'md5') -> str:
        """
        Calculate the checksum of a file.
        
        Args:
            file_path: Path to the file
            algorithm: Hash algorithm to use ('md5', 'sha1', 'sha256')
            
        Returns:
            str: Calculated checksum
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        hash_alg = None
        if algorithm.lower() == 'md5':
            hash_alg = hashlib.md5()
        elif algorithm.lower() == 'sha1':
            hash_alg = hashlib.sha1()
        elif algorithm.lower() == 'sha256':
            hash_alg = hashlib.sha256()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
        
        with open(file_path, 'rb') as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b''):
                hash_alg.update(chunk)
        
        return hash_alg.hexdigest()
    
    def validate_file(
        self, 
        file_path: str, 
        required_ext: List[str] = None,
        min_size: int = None,
        max_size: int = None
    ) -> Dict[str, Any]:
        """
        Validate a file based on extension and size.
        
        Args:
            file_path: Path to the file
            required_ext: List of allowed file extensions
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            
        Returns:
            Dict: Validation results
        """
        if not os.path.exists(file_path):
            return {
                'valid': False,
                'errors': ['File not found']
            }
        
        errors = []
        
        # Check file extension
        if required_ext:
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in required_ext:
                errors.append(f"Invalid file extension: {ext}. Expected one of: {', '.join(required_ext)}")
        
        # Check file size
        size = os.path.getsize(file_path)
        if min_size is not None and size < min_size:
            errors.append(f"File size ({size} bytes) is smaller than minimum required size ({min_size} bytes)")
        if max_size is not None and size > max_size:
            errors.append(f"File size ({size} bytes) exceeds maximum allowed size ({max_size} bytes)")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'file_name': os.path.basename(file_path),
            'file_path': file_path,
            'file_size': size,
            'file_extension': os.path.splitext(file_path)[1].lower(),
            'last_modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
        }