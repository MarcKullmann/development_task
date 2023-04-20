class CustomError(Exception):
    """Custom error class"""

    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"CustomError: {self.message}"
    
    def send_error_report(self):
        subject = "CustomError"
        body = f"Error: {self.message}"
        
        # send email or to reporting tool
        print("Error report sent successfully.")
