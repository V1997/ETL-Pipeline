import React, { useState } from 'react';
import { Card, Form, Button, Alert, ProgressBar } from 'react-bootstrap';
import { FaUpload, FaFileExcel, FaFileCsv } from 'react-icons/fa';
import { etlService } from '../services/api';

const FileUpload = ({ onUploadComplete }) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      // Validate file type
      if (!file.name.match(/\.(csv|xlsx|xls)$/)) {
        setError('Please select a valid Excel (.xlsx, .xls) or CSV file');
        setSelectedFile(null);
        return;
      }
      
      setSelectedFile(file);
      setError(null);
    }
  };

  const handleUpload = async (event) => {
    event.preventDefault();
    
    if (!selectedFile) {
      setError('Please select a file to upload');
      return;
    }
    
    setUploading(true);
    setProgress(0);
    setError(null);
    setSuccess(null);
    
    try {
      // Simulate progress (since axios doesn't update progress for small files)
      const progressInterval = setInterval(() => {
        setProgress(prev => {
          if (prev >= 90) clearInterval(progressInterval);
          return Math.min(prev + 10, 90);
        });
      }, 200);
      
      // Upload file
      const response = await etlService.uploadBatchFile(selectedFile);
      
      // Clear progress timer
      clearInterval(progressInterval);
      setProgress(100);
      
      // Set success message
      setSuccess(`File uploaded successfully! Job ID: ${response.data.job_id}`);
      
      // Reset form
      setSelectedFile(null);
      event.target.reset();
      
      // Notify parent component
      if (onUploadComplete) {
        onUploadComplete(response.data);
      }
      
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to upload file');
      setProgress(0);
    } finally {
      setUploading(false);
    }
  };

  const getFileIcon = () => {
    if (!selectedFile) return null;
    
    if (selectedFile.name.endsWith('.csv')) {
      return <FaFileCsv size={20} className="text-success me-2" />;
    } else {
      return <FaFileExcel size={20} className="text-primary me-2" />;
    }
  };

  return (
    <Card>
      <Card.Header>
        <h5 className="mb-0">Upload Sales Data</h5>
      </Card.Header>
      <Card.Body>
        {error && (
          <Alert variant="danger" onClose={() => setError(null)} dismissible>
            {error}
          </Alert>
        )}
        
        {success && (
          <Alert variant="success" onClose={() => setSuccess(null)} dismissible>
            {success}
          </Alert>
        )}
        
        <Form onSubmit={handleUpload}>
          <Form.Group className="mb-3">
            <Form.Label>Select Excel or CSV file</Form.Label>
            <div className="custom-file-upload">
              <Form.Control 
                type="file" 
                accept=".csv, .xlsx, .xls" 
                onChange={handleFileChange}
                disabled={uploading}
              />
            </div>
            <Form.Text className="text-muted">
              Supported formats: .xlsx, .xls, .csv
            </Form.Text>
          </Form.Group>
          
          {selectedFile && (
            <div className="selected-file mb-3">
              <div className="d-flex align-items-center">
                {getFileIcon()}
                <span>{selectedFile.name} ({(selectedFile.size / 1024).toFixed(2)} KB)</span>
              </div>
            </div>
          )}
          
          {uploading && (
            <ProgressBar 
              now={progress} 
              label={`${progress}%`} 
              animated 
              className="mb-3" 
            />
          )}
          
          <Button 
            variant="primary" 
            type="submit" 
            disabled={!selectedFile || uploading}
            className="d-flex align-items-center"
          >
            <FaUpload className="me-2" />
            {uploading ? 'Uploading...' : 'Upload File'}
          </Button>
        </Form>
      </Card.Body>
    </Card>
  );
};

export default FileUpload;