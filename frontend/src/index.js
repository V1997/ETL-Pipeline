import React from 'react';
import ReactDOM from 'react-dom/client'; // Import the new 'createRoot' API
import App from './App';
import './index.css'; // Optional, if you use this file for styles

// Create the root of the app
const root = ReactDOM.createRoot(document.getElementById('root'));

// Render the React app
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
