import React from 'react';
import { Card, Row, Col, Badge, ProgressBar } from 'react-bootstrap';
import { FaServer, FaDatabase, FaExchangeAlt, FaCheckCircle, FaTimesCircle } from 'react-icons/fa';
import moment from 'moment';

const SystemStatus = ({ healthStatus }) => {
  if (!healthStatus) return null;

  const formatUptime = (seconds) => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    
    let result = '';
    if (days > 0) result += `${days}d `;
    if (hours > 0) result += `${hours}h `;
    result += `${minutes}m`;
    
    return result;
  };

  const getStatusBadge = (status) => {
    if (status === 'healthy') {
      return <Badge bg="success">Healthy <FaCheckCircle /></Badge>;
    } else if (status === 'degraded') {
      return <Badge bg="warning">Degraded</Badge>;
    } else {
      return <Badge bg="danger">Unhealthy <FaTimesCircle /></Badge>;
    }
  };

  return (
    <Card className="system-status-card mb-4">
      <Card.Header>
        <h5 className="mb-0">System Status</h5>
        <small className="text-muted">
          Last checked: {moment(healthStatus.timestamp).format('YYYY-MM-DD HH:mm:ss')}
        </small>
      </Card.Header>
      <Card.Body>
        <Row>
          <Col md={6} lg={3} className="mb-3 mb-lg-0">
            <div className="d-flex align-items-center">
              <div className="status-icon me-3">
                <FaServer size={24} className="text-primary" />
              </div>
              <div>
                <div className="text-muted small">API Status</div>
                <div>{getStatusBadge(healthStatus.status)}</div>
              </div>
            </div>
          </Col>
          <Col md={6} lg={3} className="mb-3 mb-lg-0">
            <div className="d-flex align-items-center">
              <div className="status-icon me-3">
                <FaDatabase size={24} className="text-info" />
              </div>
              <div>
                <div className="text-muted small">Database</div>
                <div>{getStatusBadge(healthStatus.database_status === 'healthy' ? 'healthy' : 'unhealthy')}</div>
              </div>
            </div>
          </Col>
          <Col md={6} lg={3} className="mb-3 mb-lg-0">
            <div className="d-flex align-items-center">
              <div className="status-icon me-3">
                <FaExchangeAlt size={24} className="text-success" />
              </div>
              <div>
                <div className="text-muted small">Kafka</div>
                <div>{getStatusBadge(healthStatus.kafka_status === 'healthy' ? 'healthy' : 'unhealthy')}</div>
              </div>
            </div>
          </Col>
          <Col md={6} lg={3}>
            <div className="text-muted small mb-1">Uptime</div>
            <div className="fw-bold">{formatUptime(healthStatus.uptime_seconds)}</div>
          </Col>
        </Row>
        
        {healthStatus.system_load && (
          <Row className="mt-4">
            <Col>
              <h6 className="text-muted mb-3">System Resources</h6>
              <div className="mb-3">
                <div className="d-flex justify-content-between mb-1">
                  <span className="text-muted small">CPU Usage</span>
                  <span className="small">{healthStatus.system_load.cpu_percent}%</span>
                </div>
                <ProgressBar 
                  variant={
                    healthStatus.system_load.cpu_percent < 60 ? 'success' : 
                    healthStatus.system_load.cpu_percent < 80 ? 'warning' : 'danger'
                  }
                  now={healthStatus.system_load.cpu_percent} 
                />
              </div>
              <div className="mb-3">
                <div className="d-flex justify-content-between mb-1">
                  <span className="text-muted small">Memory Usage</span>
                  <span className="small">{healthStatus.system_load.memory_percent}%</span>
                </div>
                <ProgressBar 
                  variant={
                    healthStatus.system_load.memory_percent < 60 ? 'success' : 
                    healthStatus.system_load.memory_percent < 80 ? 'warning' : 'danger'
                  }
                  now={healthStatus.system_load.memory_percent} 
                />
              </div>
              <div>
                <div className="d-flex justify-content-between mb-1">
                  <span className="text-muted small">Disk Usage</span>
                  <span className="small">{healthStatus.system_load.disk_percent}%</span>
                </div>
                <ProgressBar 
                  variant={
                    healthStatus.system_load.disk_percent < 70 ? 'success' : 
                    healthStatus.system_load.disk_percent < 85 ? 'warning' : 'danger'
                  }
                  now={healthStatus.system_load.disk_percent} 
                />
              </div>
            </Col>
          </Row>
        )}
      </Card.Body>
    </Card>
  );
};

export default SystemStatus;