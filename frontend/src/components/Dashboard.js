import React, { useState, useEffect } from 'react';
import { salesService, systemService } from '../services/api';
import { Container, Row, Col, Card, Alert } from 'react-bootstrap';
import { Line, Bar, Pie } from 'react-chartjs-2';
import SalesMetrics from './SalesMetrics';
import SystemStatus from './SystemStatus';
import LoadingSpinner from './LoadingSpinner';

// Import Chart.js
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

const Dashboard = () => {
  const [overview, setOverview] = useState(null);
  const [regionAnalytics, setRegionAnalytics] = useState([]);
  const [itemTypeAnalytics, setItemTypeAnalytics] = useState([]);
  const [timeAnalytics, setTimeAnalytics] = useState([]);
  const [healthStatus, setHealthStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch all dashboard data
  useEffect(() => {
    const fetchDashboardData = async () => {
      try {
        setLoading(true);
        
        // Get overview data
        const overviewResponse = await salesService.getOverview();
        setOverview(overviewResponse.data);
        
        // Get analytics by region
        const regionResponse = await salesService.getAnalytics('REGION', {
          time_period: 'MONTHLY',
          limit: 100
        });
        setRegionAnalytics(regionResponse.data);
        
        // Get analytics by item type
        const itemResponse = await salesService.getAnalytics('ITEM_TYPE', {
          time_period: 'MONTHLY',
          limit: 100
        });
        setItemTypeAnalytics(itemResponse.data);
        
        // Get analytics over time
        const timeResponse = await salesService.getAnalytics('DATE', {
          time_period: 'MONTHLY',
          limit: 12
        });
        setTimeAnalytics(timeResponse.data.sort((a, b) => 
          new Date(a.period_start) - new Date(b.period_start)
        ));
        
        // Get system health
        const healthResponse = await systemService.getHealth();
        setHealthStatus(healthResponse.data);
        
        setLoading(false);
      } catch (err) {
        console.error('Error fetching dashboard data:', err);
        setError('Failed to load dashboard data. Please try again later.');
        setLoading(false);
      }
    };

    fetchDashboardData();
    
    // Set up periodic refresh for health status (every 30 seconds)
    const healthInterval = setInterval(async () => {
      try {
        const healthResponse = await systemService.getHealth();
        setHealthStatus(healthResponse.data);
      } catch (err) {
        console.error('Error fetching health status:', err);
      }
    }, 30000);
    
    return () => clearInterval(healthInterval);
  }, []);

  // Prepare charts data
  const prepareTimeSeriesData = () => {
    if (!timeAnalytics.length) return null;
    
    return {
      labels: timeAnalytics.map(item => {
        const date = new Date(item.period_start);
        return `${date.getFullYear()}-${date.getMonth() + 1}`;
      }),
      datasets: [
        {
          label: 'Revenue',
          data: timeAnalytics.map(item => item.total_sales),
          borderColor: 'rgba(75, 192, 192, 1)',
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
        },
        {
          label: 'Profit',
          data: timeAnalytics.map(item => item.total_profit),
          borderColor: 'rgba(153, 102, 255, 1)',
          backgroundColor: 'rgba(153, 102, 255, 0.2)',
        }
      ]
    };
  };

  const prepareRegionData = () => {
    if (!regionAnalytics.length) return null;
    
    return {
      labels: regionAnalytics.map(item => item.region),
      datasets: [
        {
          label: 'Profit by Region',
          data: regionAnalytics.map(item => item.profit),
          backgroundColor: [
            'rgba(255, 99, 132, 0.6)',
            'rgba(54, 162, 235, 0.6)',
            'rgba(255, 206, 86, 0.6)',
            'rgba(75, 192, 192, 0.6)',
            'rgba(153, 102, 255, 0.6)',
          ],
          borderWidth: 1,
        },
      ],
    };
  };

  const prepareItemTypeData = () => {
    if (!itemTypeAnalytics.length) return null;
    
    return {
      labels: itemTypeAnalytics.map(item => item.item_type),
      datasets: [
        {
          label: 'Profit by Item Type',
          data: itemTypeAnalytics.map(item => item.profit),
          backgroundColor: [
            'rgba(255, 99, 132, 0.6)',
            'rgba(54, 162, 235, 0.6)',
            'rgba(255, 206, 86, 0.6)',
            'rgba(75, 192, 192, 0.6)',
            'rgba(153, 102, 255, 0.6)',
          ],
          borderWidth: 1,
        },
      ],
    };
  };

  if (loading) {
    return <LoadingSpinner message="Loading dashboard data..." />;
  }

  if (error) {
    return (
      <Container className="mt-4">
        <Alert variant="danger">{error}</Alert>
      </Container>
    );
  }

  return (
    <Container fluid className="dashboard-container p-4">
      <h1 className="mb-4">Sales Data Dashboard</h1>
      
      {/* System Status */}
      <Row className="mb-4">
        <Col>
          <SystemStatus healthStatus={healthStatus} />
        </Col>
      </Row>

      {/* Key Metrics */}
      <Row className="mb-4">
        <Col>
          <SalesMetrics overview={overview} />
        </Col>
      </Row>

      {/* Charts Row */}
      <Row className="mb-4">
        {/* Time Series Chart */}
        <Col lg={8} className="mb-4">
          <Card>
            <Card.Header>Sales Performance Over Time</Card.Header>
            <Card.Body>
              {prepareTimeSeriesData() && (
                <Line 
                  data={prepareTimeSeriesData()}
                  options={{
                    responsive: true,
                    plugins: {
                      legend: { position: 'top' },
                      title: { display: true, text: 'Monthly Sales Performance' }
                    }
                  }}
                />
              )}
            </Card.Body>
          </Card>
        </Col>

        {/* Profit by Region */}
        <Col lg={4} className="mb-4">
          <Card>
            <Card.Header>Profit by Region</Card.Header>
            <Card.Body>
              {prepareRegionData() && (
                <Pie 
                  data={prepareRegionData()}
                  options={{
                    responsive: true,
                    plugins: {
                      legend: { position: 'right' }
                    }
                  }}
                />
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>

      <Row>
        {/* Profit by Item Type */}
        <Col>
          <Card>
            <Card.Header>Profit by Item Type</Card.Header>
            <Card.Body>
              {prepareItemTypeData() && (
                <Bar 
                  data={prepareItemTypeData()}
                  options={{
                    responsive: true,
                    plugins: {
                      legend: { display: false },
                      title: { display: true, text: 'Top Performing Item Types' }
                    }
                  }}
                />
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </Container>
  );
};

export default Dashboard;