import React from 'react';
import { Row, Col, Card } from 'react-bootstrap';
import { FaChartLine, FaMoneyBillWave, FaBoxOpen, FaCalendarCheck } from 'react-icons/fa';
import CountUp from 'react-countup';
import moment from 'moment';

const MetricCard = ({ title, value, icon, color, formatter, prefix, suffix }) => {
  return (
    <Card className="h-100 metric-card">
      <Card.Body>
        <Row>
          <Col xs={8}>
            <h6 className="text-muted">{title}</h6>
            <h4 className="metric-value">
              {prefix}
              <CountUp 
                end={value} 
                duration={2.5} 
                separator="," 
                decimals={formatter === 'currency' ? 2 : 0}
                decimal="."
              />
              {suffix}
            </h4>
          </Col>
          <Col xs={4} className="d-flex justify-content-end align-items-center">
            <div className={`metric-icon-container bg-${color} text-white`}>
              {icon}
            </div>
          </Col>
        </Row>
      </Card.Body>
    </Card>
  );
};

const SalesMetrics = ({ overview }) => {
  if (!overview) return null;

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', { 
      style: 'currency', 
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  return (
    <Card className="mb-4">
      <Card.Header>
        <h5 className="mb-0">Key Sales Metrics</h5>
        <small className="text-muted">
          Last updated: {overview.latest_update ? moment(overview.latest_update).format('YYYY-MM-DD HH:mm') : 'N/A'}
        </small>
      </Card.Header>
      <Card.Body>
        <Row>
          <Col lg={3} md={6} className="mb-3 mb-lg-0">
            <MetricCard 
              title="Total Revenue" 
              value={overview.total_revenue || 0}
              icon={<FaMoneyBillWave size={25} />}
              color="success"
              formatter="currency"
              prefix="$"
            />
          </Col>
          <Col lg={3} md={6} className="mb-3 mb-lg-0">
            <MetricCard 
              title="Total Profit" 
              value={overview.total_profit || 0}
              icon={<FaChartLine size={25} />}
              color="primary"
              formatter="currency"
              prefix="$"
            />
          </Col>
          <Col lg={3} md={6} className="mb-3 mb-lg-0">
            <MetricCard 
              title="Units Sold" 
              value={overview.total_units_sold || 0}
              icon={<FaBoxOpen size={25} />}
              color="warning"
              formatter="number"
            />
          </Col>
          <Col lg={3} md={6}>
            <MetricCard 
              title="Total Records" 
              value={overview.total_records || 0}
              icon={<FaCalendarCheck size={25} />}
              color="info"
              formatter="number"
            />
          </Col>
        </Row>
      </Card.Body>
    </Card>
  );
};

export default SalesMetrics;