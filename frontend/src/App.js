import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import { Container } from 'react-bootstrap';
import Dashboard from './components/Dashboard';
import FileUpload from './components/FileUpload';
import './App.css';

const App = () => {
  return (
    <Router>
      <Container fluid className="p-0">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/upload" element={<FileUpload />} />
        </Routes>
      </Container>
    </Router>
  );
};

export default App;
