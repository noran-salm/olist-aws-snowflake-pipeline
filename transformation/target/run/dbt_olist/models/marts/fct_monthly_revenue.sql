-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into OLIST_DW.MARTS.fct_monthly_revenue as DBT_INTERNAL_DEST
        using OLIST_DW.MARTS.fct_monthly_revenue__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_year_month = DBT_INTERNAL_DEST.order_year_month
                ) and (
                    DBT_INTERNAL_SOURCE.product_category = DBT_INTERNAL_DEST.product_category
                ) and (
                    DBT_INTERNAL_SOURCE.customer_state = DBT_INTERNAL_DEST.customer_state
                ) and (
                    DBT_INTERNAL_SOURCE.seller_state = DBT_INTERNAL_DEST.seller_state
                )

    
    when matched then update set
        "ORDER_YEAR_MONTH" = DBT_INTERNAL_SOURCE."ORDER_YEAR_MONTH","PRODUCT_CATEGORY" = DBT_INTERNAL_SOURCE."PRODUCT_CATEGORY","CUSTOMER_STATE" = DBT_INTERNAL_SOURCE."CUSTOMER_STATE","CUSTOMER_STATE_NAME" = DBT_INTERNAL_SOURCE."CUSTOMER_STATE_NAME","CUSTOMER_REGION" = DBT_INTERNAL_SOURCE."CUSTOMER_REGION","SELLER_STATE" = DBT_INTERNAL_SOURCE."SELLER_STATE","TOTAL_ORDERS" = DBT_INTERNAL_SOURCE."TOTAL_ORDERS","TOTAL_ITEMS" = DBT_INTERNAL_SOURCE."TOTAL_ITEMS","REVENUE_BRL" = DBT_INTERNAL_SOURCE."REVENUE_BRL","AVG_REVIEW" = DBT_INTERNAL_SOURCE."AVG_REVIEW","LATE_DELIVERIES" = DBT_INTERNAL_SOURCE."LATE_DELIVERIES","AVG_DELIVERY_DAYS" = DBT_INTERNAL_SOURCE."AVG_DELIVERY_DAYS"
    

    when not matched then insert
        ("ORDER_YEAR_MONTH", "PRODUCT_CATEGORY", "CUSTOMER_STATE", "CUSTOMER_STATE_NAME", "CUSTOMER_REGION", "SELLER_STATE", "TOTAL_ORDERS", "TOTAL_ITEMS", "REVENUE_BRL", "AVG_REVIEW", "LATE_DELIVERIES", "AVG_DELIVERY_DAYS")
    values
        ("ORDER_YEAR_MONTH", "PRODUCT_CATEGORY", "CUSTOMER_STATE", "CUSTOMER_STATE_NAME", "CUSTOMER_REGION", "SELLER_STATE", "TOTAL_ORDERS", "TOTAL_ITEMS", "REVENUE_BRL", "AVG_REVIEW", "LATE_DELIVERIES", "AVG_DELIVERY_DAYS")

;
    commit;