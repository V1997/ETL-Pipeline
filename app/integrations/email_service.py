
import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import jinja2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class EmailConfig:
    """Email configuration settings from environment variables."""
    
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.example.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    DEFAULT_SENDER = os.getenv("EMAIL_DEFAULT_SENDER", "etl-pipeline@example.com")
    ADMIN_EMAIL = os.getenv("EMAIL_ADMIN", "admin@example.com")
    TEMPLATE_DIR = os.getenv("EMAIL_TEMPLATE_DIR", os.path.join(os.getcwd(), "app/templates/email"))

class EmailService:
    """
    Email notification service for ETL pipeline.
    
    This class handles:
    - Sending email notifications for various events
    - HTML email template rendering
    - Email attachments
    - Error reporting via email
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the email service.
        
        Args:
            config: Optional configuration overrides
        """
        self.config = {
            'smtp_server': EmailConfig.SMTP_SERVER,
            'smtp_port': EmailConfig.SMTP_PORT,
            'smtp_username': EmailConfig.SMTP_USERNAME,
            'smtp_password': EmailConfig.SMTP_PASSWORD,
            'use_tls': EmailConfig.USE_TLS,
            'default_sender': EmailConfig.DEFAULT_SENDER,
            'admin_email': EmailConfig.ADMIN_EMAIL,
            'template_dir': EmailConfig.TEMPLATE_DIR
        }
        
        if config:
            self.config.update(config)
        
        # Create template environment
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(self.config['template_dir']),
            autoescape=jinja2.select_autoescape(['html', 'xml'])
        )
        
        logger.info(f"Email service initialized with SMTP server: {self.config['smtp_server']}")
    
    def send_email(
        self,
        recipients: Union[str, List[str]],
        subject: str,
        body: str,
        sender: str = None,
        cc: Union[str, List[str]] = None,
        bcc: Union[str, List[str]] = None,
        html: bool = False,
        attachments: List[str] = None
    ) -> bool:
        """
        Send an email.
        
        Args:
            recipients: Email recipient(s)
            subject: Email subject
            body: Email body
            sender: Email sender (uses default if not provided)
            cc: Carbon copy recipient(s)
            bcc: Blind carbon copy recipient(s)
            html: Whether the body is HTML
            attachments: List of file paths to attach
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        # Normalize inputs
        if isinstance(recipients, str):
            recipients = [recipients]
            
        if isinstance(cc, str):
            cc = [cc]
        elif cc is None:
            cc = []
            
        if isinstance(bcc, str):
            bcc = [bcc]
        elif bcc is None:
            bcc = []
            
        sender = sender or self.config['default_sender']
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        
        if cc:
            msg['Cc'] = ', '.join(cc)
        
        # Attach body
        if html:
            msg.attach(MIMEText(body, 'html'))
        else:
            msg.attach(MIMEText(body, 'plain'))
        
        # Add attachments
        if attachments:
            for file_path in attachments:
                try:
                    with open(file_path, 'rb') as file:
                        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
                        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                        msg.attach(part)
                except Exception as e:
                    logger.error(f"Failed to attach file {file_path}: {str(e)}")
        
        try:
            # Connect to SMTP server
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            
            if self.config['use_tls']:
                server.starttls()
            
            # Login if credentials are provided
            if self.config['smtp_username'] and self.config['smtp_password']:
                server.login(self.config['smtp_username'], self.config['smtp_password'])
            
            # Combine all recipients for sending
            all_recipients = recipients + cc + bcc
            
            # Send email
            server.sendmail(sender, all_recipients, msg.as_string())
            server.quit()
            
            logger.info(f"Email sent to {', '.join(recipients)}: {subject}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
    
    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render an email template with the given context.
        
        Args:
            template_name: Name of the template file
            context: Template variables
            
        Returns:
            str: Rendered template
        """
        try:
            template = self.template_env.get_template(template_name)
            return template.render(**context)
        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {str(e)}")
            raise
    
    def send_template_email(
        self,
        recipients: Union[str, List[str]],
        subject: str,
        template_name: str,
        context: Dict[str, Any],
        sender: str = None,
        cc: Union[str, List[str]] = None,
        bcc: Union[str, List[str]] = None,
        attachments: List[str] = None
    ) -> bool:
        """
        Send an email using a template.
        
        Args:
            recipients: Email recipient(s)
            subject: Email subject
            template_name: Template file name
            context: Template variables
            sender: Email sender (uses default if not provided)
            cc: Carbon copy recipient(s)
            bcc: Blind carbon copy recipient(s)
            attachments: List of file paths to attach
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        try:
            html_body = self.render_template(template_name, context)
            return self.send_email(recipients, subject, html_body, sender, cc, bcc, True, attachments)
        except Exception as e:
            logger.error(f"Failed to send template email: {str(e)}")
            return False
    
    def send_batch_status_notification(
        self,
        batch_id: str,
        status: str,
        statistics: Dict[str, Any],
        recipients: Union[str, List[str]] = None
    ) -> bool:
        """
        Send a notification about batch processing status.
        
        Args:
            batch_id: Batch ID
            status: Batch status
            statistics: Batch processing statistics
            recipients: Email recipient(s)
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        recipients = recipients or self.config['admin_email']
        
        # Format subject based on status
        if status.lower() == 'completed':
            subject = f"ETL Pipeline: Batch {batch_id} completed successfully"
        elif status.lower() == 'failed':
            subject = f"ETL Pipeline: Batch {batch_id} failed"
        else:
            subject = f"ETL Pipeline: Batch {batch_id} status update - {status}"
        
        # Prepare template context
        context = {
            'batch_id': batch_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'statistics': statistics
        }
        
        # Send email using template
        return self.send_template_email(
            recipients=recipients,
            subject=subject,
            template_name='batch_status.html',
            context=context
        )
    
    def send_error_notification(
        self,
        error_type: str,
        error_message: str,
        component: str,
        batch_id: str = None,
        record_id: str = None,
        details: Dict[str, Any] = None,
        recipients: Union[str, List[str]] = None
    ) -> bool:
        """
        Send a notification about an error.
        
        Args:
            error_type: Type of error
            error_message: Error message
            component: Component where the error occurred
            batch_id: Associated batch ID
            record_id: Associated record ID
            details: Additional error details
            recipients: Email recipient(s)
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        recipients = recipients or self.config['admin_email']
        subject = f"ETL Pipeline Error Alert: {error_type} in {component}"
        
        # Prepare template context
        context = {
            'error_type': error_type,
            'error_message': error_message,
            'component': component,
            'batch_id': batch_id,
            'record_id': record_id,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details or {}
        }
        
        # Send email using template
        return self.send_template_email(
            recipients=recipients,
            subject=subject,
            template_name='error_notification.html',
            context=context
        )
    
    def send_daily_summary(
        self,
        date: datetime,
        statistics: Dict[str, Any],
        recipients: Union[str, List[str]] = None,
        report_path: str = None
    ) -> bool:
        """
        Send a daily summary report.
        
        Args:
            date: Report date
            statistics: Processing statistics for the day
            recipients: Email recipient(s)
            report_path: Path to report file to attach
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        recipients = recipients or self.config['admin_email']
        date_str = date.strftime('%Y-%m-%d')
        subject = f"ETL Pipeline Daily Summary: {date_str}"
        
        # Prepare template context
        context = {
            'date': date_str,
            'statistics': statistics,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Prepare attachments
        attachments = []
        if report_path and os.path.exists(report_path):
            attachments.append(report_path)
        
        # Send email using template
        return self.send_template_email(
            recipients=recipients,
            subject=subject,
            template_name='daily_summary.html',
            context=context,
            attachments=attachments
        )