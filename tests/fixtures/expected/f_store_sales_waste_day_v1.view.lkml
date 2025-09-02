# Un-hide and use this explore, or copy the joins into another explore, to get all the fully nested relationships from this view
explore: f_store_sales_waste_day_v1 {
  hidden: yes
    join: f_store_sales_waste_day_v1__waste {
      view_label: "F Store Sales Waste Day V1: Waste"
      sql: LEFT JOIN UNNEST(${f_store_sales_waste_day_v1.waste}) as f_store_sales_waste_day_v1__waste ;;
      relationship: one_to_many
    }
    join: f_store_sales_waste_day_v1__sales {
      view_label: "F Store Sales Waste Day V1: Sales"
      sql: LEFT JOIN UNNEST(${f_store_sales_waste_day_v1.sales}) as f_store_sales_waste_day_v1__sales ;;
      relationship: one_to_many
    }
    join: f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys {
      view_label: "F Store Sales Waste Day V1: Sales F Sale Receipt Pseudo Keys"
      sql: LEFT JOIN UNNEST(${f_store_sales_waste_day_v1__sales.f_sale_receipt_pseudo_keys}) as f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys ;;
      relationship: one_to_many
    }
}
view: f_store_sales_waste_day_v1 {
  sql_table_name: `ac16-p-conlaybi-prd-4257.consumer_sales_secure_versioned.f_store_sales_waste_day_v1` ;;

  dimension_group: d {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.d_date ;;
  }
  dimension: d_item_key {
    type: number
    sql: ${TABLE}.d_item_key ;;
  }
  dimension: d_selling_entity_key {
    type: number
    sql: ${TABLE}.d_selling_entity_key ;;
  }
  dimension: d_store_local_item_key {
    type: number
    sql: ${TABLE}.d_store_local_item_key ;;
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
  dimension: waste {
    hidden: yes
    sql: ${TABLE}.waste ;;
  }
  measure: count {
    type: count
  }
}

view: f_store_sales_waste_day_v1__waste {

  dimension: d_store_waste_info_key {
    type: number
    sql: ${TABLE}.d_store_waste_info_key ;;
  }
  dimension: f_store_sales_waste_day_v1__waste {
    type: string
    hidden: yes
    sql: f_store_sales_waste_day_v1__waste ;;
  }
  dimension: number_of_items_or_weight_in_kg {
    type: number
    sql: ${TABLE}.number_of_items_or_weight_in_kg ;;
  }
  dimension: purchase_amount {
    type: number
    sql: ${TABLE}.purchase_amount ;;
  }
  dimension: total_amount {
    type: number
    sql: ${TABLE}.total_amount ;;
  }
}

view: f_store_sales_waste_day_v1__sales {

  dimension: d_checkout_method_key {
    type: number
    sql: ${TABLE}.d_checkout_method_key ;;
  }
  dimension: f_sale_receipt_pseudo_keys {
    hidden: yes
    sql: ${TABLE}.f_sale_receipt_pseudo_keys ;;
  }
  dimension: f_sale_receipt_pseudo_keys_sketch {
    type: string
    description: "HLL++-sketch to efficiently approximate number of visits."
    sql: ${TABLE}.f_sale_receipt_pseudo_keys_sketch ;;
  }
  dimension: f_store_sales_waste_day_v1__sales {
    type: string
    hidden: yes
    sql: f_store_sales_waste_day_v1__sales ;;
  }
  dimension: is_commission_item {
    type: yesno
    sql: ${TABLE}.is_commission_item ;;
  }
  dimension: margin_amount {
    type: number
    sql: ${TABLE}.margin_amount ;;
  }
  dimension: number_of_items {
    type: number
    sql: ${TABLE}.number_of_items ;;
  }
  dimension: sale_receipt_line_type_code {
    type: string
    sql: ${TABLE}.sale_receipt_line_type_code ;;
  }
  dimension: so_campaign_type_id {
    type: string
    sql: ${TABLE}.so_campaign_type_id ;;
  }
  dimension: store_sale_amount {
    type: number
    sql: ${TABLE}.store_sale_amount ;;
  }
}

view: f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys {

  dimension: f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys {
    type: number
    description: "Array of salted keys for f_sale_receipt_key on receipt-line-level. Used to calculate unique number of visits."
    sql: f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys ;;
  }
}
