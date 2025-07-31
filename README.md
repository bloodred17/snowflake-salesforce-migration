# Salesforce-Snowflake Integration

Syncs order data between Salesforce and Snowflake, processing orders from the past 7 days and updating existing records with current fulfillment information.

## Features

- Processes orders by sales order date (past 7 days)
- Updates existing orders and items with current fulfillment data
- Tracks both open and posted orders
- Real-time updates for shipped quantities, prices, and discounts

## Configuration

Set environment variables or use the default values in `config.py`:

### Salesforce
- `SF_USERNAME` - Salesforce username
- `SF_PASSWORD` - Salesforce password  
- `SF_SECURITY_TOKEN` - Salesforce security token
- `SF_DOMAIN` - Salesforce domain (test/login)

### Snowflake
- `SNOWFLAKE_USER` - Snowflake username
- `SNOWFLAKE_PASSWORD` - Snowflake password
- `SNOWFLAKE_ACCOUNT` - Snowflake account
- `SNOWFLAKE_WAREHOUSE` - Snowflake warehouse
- `SNOWFLAKE_DATABASE` - Snowflake database
- `SNOWFLAKE_SCHEMA` - Snowflake schema
- `SNOWFLAKE_ROLE` - Snowflake role

### Application
- `APP_MAX_RETRIES` - Max retry attempts (default: 99)
- `APP_RETRY_WAIT` - Retry wait time in seconds (default: 5)
- `APP_CYCLE_WAIT` - Cycle wait time in seconds (default: 1000)
- `APP_LOG_LEVEL` - Logging level (default: INFO)
- `APP_LOG_FILE` - Log file name (default: sf_snowflake_integration.log)

## Steps to Run

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   or 
   ```bash
   uv sync
   ```

2. **Set Environment Variables** (Optional)
   ```bash
   export SF_USERNAME=your_username
   export SF_PASSWORD=your_password
   export SF_SECURITY_TOKEN=your_token
   ```

3. **Run the Integration**
   ```bash
   python main.py
   ```
   or
   ```bash
   uv run python main.py
   ```

4. **Check Logs**
   ```bash
   tail -f sf_snowflake_integration.log
   ```

## Usage

```bash
# Set environment variables
export SF_USERNAME=your_username
export SF_PASSWORD=your_password
export SF_SECURITY_TOKEN=your_token

# Run the integration
python main.py
```

## How It Works

1. Fetches orders from Snowflake (past 7 days by sales order date)
2. Matches customer data to Salesforce accounts
3. Creates new orders/items or updates existing ones with current data
4. Updates order status (Open/Closed) and fulfillment information
