explore: f_store_sales_day_selling_entity_v1 {
  hidden: yes
    join: f_store_sales_day_selling_entity_v1__sales {
      view_label: "F Store Sales Day Selling Entity V1: Sales"
      sql: LEFT JOIN UNNEST(${f_store_sales_day_selling_entity_v1.sales}) AS f_store_sales_day_selling_entity_v1__sales ;;
      relationship: one_to_many
    }

    join: f_store_sales_day_selling_entity_v1__sales__f_sale_receipt_pseudo_keys {
      view_label: "F Store Sales Day Selling Entity V1: Sales F Sale Receipt Pseudo Keys"
      sql: LEFT JOIN UNNEST(${f_store_sales_day_selling_entity_v1__sales.f_sale_receipt_pseudo_keys}) AS f_store_sales_day_selling_entity_v1__sales__f_sale_receipt_pseudo_keys ;;
      relationship: one_to_many
    }
}

view: f_store_sales_day_selling_entity_v1 {
  sql_table_name: `ac16-p-conlaybi-prd-4257.consumer_sales_looker.f_store_sales_day_selling_entity_v1` ;;
 
  dimension_group: d {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.d_date ;;
  }
  
  dimension: d_selling_entity_key {
    type: number
    sql: ${TABLE}.d_selling_entity_key ;;
  }
  
  dimension: md_audit_seq {
    type: string
    sql: ${TABLE}.md_audit_seq ;;
  }
  
  dimension_group: md_insert_dttm {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: datetime
    sql: ${TABLE}.md_insert_dttm ;;
  }
  
  dimension: sales {
    hidden: yes
    sql: ${TABLE}.sales ;;
  }
  measure: count {
    type: count
  }
}
 
view: f_store_sales_day_selling_entity_v1__sales {
 
  dimension: commission_amount {
    type: number
    description: "commission amount for the order."
    sql: commission_amount ;;
  }
  
  dimension: consumer_type {
    type: string
    description: "Type of consumer making the purchase."
    sql: consumer_type ;;
  }
  
  dimension: d_checkout_method_key {
    type: number
    sql: d_checkout_method_key ;;
  }
  
  dimension: d_online_order_delivery_method_code {
    type: string
    description: "Delivery method code for online orders."
    sql: d_online_order_delivery_method_code ;;
  }
  
  dimension: d_online_order_picking_location_code_majority {
    type: string
    description: "Majority picking location code for online orders."
    sql: d_online_order_picking_location_code_majority ;;
  }
  
  dimension: d_sale_receipt_line_type_code {
    type: string
    sql: d_sale_receipt_line_type_code ;;
  }
  
  dimension: d_shopping_mission_id {
    type: string
    description: "Identifier for the shopping mission."
    sql: d_shopping_mission_id ;;
  }
  
  dimension: d_so_campaign_type_id {
    type: string
    sql: d_so_campaign_type_id ;;
  }
  
  dimension: d_unit_of_measure_code {
    type: string
    description: "Unit of measure code for the item."
    sql: d_unit_of_measure_code ;;
  }
  
  dimension: delivery_fee_amount {
    type: number
    description: "Delivery fee amount for the order."
    sql: delivery_fee_amount ;;
  }
  
  dimension: deposit_amount {
    type: number
    description: "deposit amount for the order."
    sql: deposit_amount ;;
  }
  
  dimension: f_sale_receipt_pseudo_keys {
    hidden: yes
    sql: f_sale_receipt_pseudo_keys ;;
  }
  
  dimension: f_sale_receipt_pseudo_keys_sketch {
    type: string
    description: "HLL++-sketch to efficiently approximate number of visits."
    sql: f_sale_receipt_pseudo_keys_sketch ;;
  }
  
  dimension: f_store_sales_day_selling_entity_v1__sales {
    type: string
    hidden: yes
    sql: f_store_sales_day_selling_entity_v1__sales ;;
  }
  
  dimension: is_commission_item {
    type: yesno
    sql: is_commission_item ;;
  }
  
  dimension: margin_amount {
    type: number
    sql: margin_amount ;;
  }
  
  dimension: number_of_items {
    type: number
    sql: number_of_items ;;
  }
  
  dimension: purchase_amount {
    type: number
    description: "purchase amount for the order."
    sql: purchase_amount ;;
  }
  
  dimension: store_sale_amount {
    type: number
    sql: store_sale_amount ;;
  }
  
  dimension: store_sale_vat_amount {
    type: number
    sql: store_sale_vat_amount ;;
  }
}
 
view: f_store_sales_day_selling_entity_v1__sales__f_sale_receipt_pseudo_keys {
  dimension: f_store_sales_day_selling_entity_v1__sales__f_sale_receipt_pseudo_keys {
    type: number
    description: "Array of salted keys for f_sale_receipt_key on receipt-line-level. Used to calculate unique number of visits."
    sql: f_store_sales_day_selling_entity_v1__sales__f_sale_receipt_pseudo_keys ;;
  }
}
 