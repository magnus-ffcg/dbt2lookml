import json
import os
import pytest
from unittest.mock import mock_open, patch, MagicMock
from pathlib import Path
from dbt2looker_bigquery.cli import generate, init_argparser

class TestIntegration:

    def test_nested_types_integration(self):
        
        expected_content = '''
view: f_store_sales_waste_day_v1 {
  label: "Conlaybi Consumer Sales Secure Versioned  F Store Sales Waste Day"
  sql_table_name: `ac16-p-conlaybi-prd-4257`.`consumer_sales_secure_versioned`.`f_store_sales_waste_day_v1` ;;

  dimension: d_date_iso_year {
    label: "D ISO Year"
    type: number
    sql: Extract(isoyear from ${TABLE}.d_date) ;;
    description: "iso year for d_date"
    group_label: "D Date"
    value_format_name: id
  }

  dimension: d_date_iso_week_of_year {
    label: "D ISO Week Of Year"
    type: number
    sql: Extract(isoweek from ${TABLE}.d_date) ;;
    description: "iso year for d_date"
    group_label: "D Date"
    value_format_name: id
  }

  dimension: d_selling_entity_key {
    type: number
    sql: ${TABLE}.d_selling_entity_key ;;
    description: ""
  }

  dimension: d_item_key {
    type: number
    sql: ${TABLE}.d_item_key ;;
    description: ""
  }

  dimension: d_store_local_item_key {
    type: number
    sql: ${TABLE}.d_store_local_item_key ;;
    description: ""
  }

  dimension: sales {
    sql: ${TABLE}.sales ;;
    description: ""
    hidden: yes
    tags: ["array"]
  }

  dimension: waste {
    sql: ${TABLE}.waste ;;
    description: ""
    hidden: yes
    tags: ["array"]
  }

  dimension: md_audit_seq {
    type: string
    sql: ${TABLE}.md_audit_seq ;;
    description: ""
  }

  dimension_group: d {
    label: "D"
    type: date
    sql: ${TABLE}.d_date ;;
    description: ""
    datatype: date
    timeframes: [
      raw,
      date,
      day_of_month,
      day_of_week,
      day_of_week_index,
      week,
      week_of_year,
      month,
      month_num,
      month_name,
      quarter,
      quarter_of_year,
      year,
    ]
    group_label: "D Date"
    convert_tz: no
  }

  dimension_group: md_insert_dttm {
    label: "Md Insert Dttm"
    type: time
    sql: ${TABLE}.md_insert_dttm ;;
    description: ""
    datatype: datetime
    timeframes: [
      raw,
      time,
      time_of_day,
      date,
      week,
      month,
      quarter,
      year,
    ]
    group_label: "Md Insert Dttm"
    convert_tz: yes
  }

  set: s_d {
    fields: [
      d_raw,
      d_date,
      d_day_of_month,
      d_day_of_week,
      d_day_of_week_index,
      d_week,
      d_week_of_year,
      d_month,
      d_month_num,
      d_month_name,
      d_quarter,
      d_quarter_of_year,
      d_year,
      d_date_iso_year,
      d_date_iso_week_of_year,
    ]
  }

  set: s_md_insert_dttm {
    fields: [
      md_insert_dttm_raw,
      md_insert_dttm_time,
      md_insert_dttm_time_of_day,
      md_insert_dttm_date,
      md_insert_dttm_week,
      md_insert_dttm_month,
      md_insert_dttm_quarter,
      md_insert_dttm_year,
    ]
  }
}

view: f_store_sales_waste_day_v1__sales {
  label: "Conlaybi Consumer Sales Secure Versioned  F Store Sales Waste Day"

  dimension: d_checkout_method_key {
    type: number
    sql: d_checkout_method_key ;;
    description: ""
  }

  dimension: sale_receipt_line_type_code {
    type: string
    sql: sale_receipt_line_type_code ;;
    description: ""
  }

  dimension: so_campaign_type_id {
    type: string
    sql: so_campaign_type_id ;;
    description: ""
  }

  dimension: is_commission_item {
    type: yesno
    sql: is_commission_item ;;
    description: ""
  }

  dimension: number_of_items {
    type: number
    sql: number_of_items ;;
    description: ""
  }

  dimension: store_sale_amount {
    type: number
    sql: store_sale_amount ;;
    description: ""
  }

  dimension: margin_amount {
    type: number
    sql: margin_amount ;;
    description: ""
  }

  dimension: f_sale_receipt_pseudo_keys {
    sql: f_sale_receipt_pseudo_keys ;;
    description: "Array of salted keys for f_sale_receipt_key on receipt-line-level. Used to calculate unique number of visits."
    hidden: yes
    tags: ["array"]
  }

  dimension: f_sale_receipt_pseudo_keys_sketch {
    type: string
    sql: f_sale_receipt_pseudo_keys_sketch ;;
    description: "HLL++-sketch to efficiently approximate number of visits."
  }
}

view: f_store_sales_waste_day_v1__sales__f_sale_receipt_pseudo_keys {
  label: "Conlaybi Consumer Sales Secure Versioned  F Store Sales Waste Day"

  dimension: f_sale_receipt_pseudo_keys {
    type: number
    sql: f_sale_receipt_pseudo_keys ;;
    description: "Array of salted keys for f_sale_receipt_key on receipt-line-level. Used to calculate unique number of visits."
  }
}

view: f_store_sales_waste_day_v1__waste {
  label: "Conlaybi Consumer Sales Secure Versioned  F Store Sales Waste Day"

  dimension: d_store_waste_info_key {
    type: number
    sql: d_store_waste_info_key ;;
    description: ""
  }

  dimension: number_of_items_or_weight_in_kg {
    type: number
    sql: number_of_items_or_weight_in_kg ;;
    description: ""
  }

  dimension: purchase_amount {
    type: number
    sql: purchase_amount ;;
    description: ""
  }

  dimension: total_amount {
    type: number
    sql: total_amount ;;
    description: ""
  }
}        
'''
        
        # Initialize and run CLI
        parser = init_argparser()
        args = parser.parse_args([
            "--target-dir", 'samples/',
            "--output-dir", 'output/tests/',
            "--select", 'conlaybi_consumer_sales_secure_versioned__f_store_sales_waste_day',
            "--use-table-name",
            "--skip-explore-joins",
        ])
        generate(args)

        assert os.path.exists('output/tests/conlaybi/consumer_sales_secure_versioned/f_store_sales_waste_day_v1.view.lkml')
        
        with open('output/tests/conlaybi/consumer_sales_secure_versioned/f_store_sales_waste_day_v1.view.lkml') as f:
            content = f.read()
        
        assert content.strip() == expected_content.strip()

    