import axios from 'axios';

// Base API URL
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000/api/v1';

// Create axios instance
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000, // 30 seconds timeout
});

// API services for ETL operations
export const etlService = {
  // Upload batch file
  uploadBatchFile: async (file) => {
    const formData = new FormData();
    formData.append('file', file);
    return apiClient.post('/etl/batch-upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
  },
  
  // Stream a single record
  streamRecord: async (record) => {
    return apiClient.post('/etl/stream-record', record);
  },


  // Get ETL jobs with optional filters
  getJobs: async (filters = {}) => {
    return apiClient.get('/etl/jobs', {
      params: filters
    });
  },
  
  // Get a specific ETL job
  getJob: async (jobId) => {
    return apiClient.get(`/etl/jobs/${jobId}`);
  },
  
  // Get ETL errors with optional filters
  getErrors: async (filters = {}) => {
    return apiClient.get('/etl/errors', {
      params: filters
    });
  }
};

// API services for sales data
export const salesService = {
  // Get sales records with optional filters
  getSalesRecords: async (filters = {}) => {
    return apiClient.get('/sales', {
      params: filters
    });
  },
  
  // Get sales analytics 
  getAnalytics: async (dimensionType, params = {}) => {
    return apiClient.get('/sales/analytics', {
      params: {
        dimension_type: dimensionType,
        ...params
      }
    });
  },
  
  // Get sales overview with key metrics
  getOverview: async () => {
    return apiClient.get('/sales/overview');
  }
};

// API service for system health
export const systemService = {
  // Check system health
  getHealth: async () => {
    return apiClient.get('/health');
  }
};

// Request interceptor for API calls
apiClient.interceptors.request.use(
  config => {
    // You can add auth tokens here if needed
    return config;
  },
  error => {
    return Promise.reject(error);
  }
);

// Response interceptor for API calls
apiClient.interceptors.response.use(
  response => {
    return response;
  },
  error => {
    const errorMessage = error.response?.data?.detail || 'An unexpected error occurred';
    console.error('API Error:', errorMessage);
    return Promise.reject(error);
  }
);

export default { etlService, salesService, systemService };