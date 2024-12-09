import json
import os
import pytest
from unittest.mock import mock_open, patch, MagicMock
from pathlib import Path
from dbt2lookml.cli import Cli

class TestIntegration:

    def test_integration_skip_explore_joins_and_use_table_name(self):
        
        expected_content = '''
view: fact_daily_sales_v1 {
  label: "Example Retail Data  Fact Daily Sales"
  sql_table_name: `example-project-123`.`retail_data`.`fact_daily_sales_v1` ;;

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

  dimension: dim_store_key {
    type: number
    sql: ${TABLE}.dim_store_key ;;
    description: ""
  }

  dimension: dim_product_key {
    type: number
    sql: ${TABLE}.dim_product_key ;;
    description: ""
  }

  dimension: dim_store_product_key {
    type: number
    sql: ${TABLE}.dim_store_product_key ;;
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

view: fact_daily_sales_v1__sales {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_payment_method_key {
    type: number
    sql: dim_payment_method_key ;;
    description: ""
  }

  dimension: transaction_type_code {
    type: string
    sql: transaction_type_code ;;
    description: ""
  }

  dimension: promotion_type_id {
    type: string
    sql: promotion_type_id ;;
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

  dimension: sales_amount {
    type: number
    sql: sales_amount ;;
    description: ""
  }

  dimension: profit_amount {
    type: number
    sql: profit_amount ;;
    description: ""
  }

  dimension: fact_transaction_keys {
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
    hidden: yes
    tags: ["array"]
  }

  dimension: fact_transaction_keys_sketch {
    type: string
    sql: fact_transaction_keys_sketch ;;
    description: "HLL++-sketch to efficiently approximate number of visits."
  }
}

view: fact_daily_sales_v1__sales__fact_transaction_keys {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: fact_transaction_keys {
    type: number
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
  }
}

view: fact_daily_sales_v1__waste {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_waste_reason_key {
    type: number
    sql: dim_waste_reason_key ;;
    description: ""
  }

  dimension: quantity_or_weight_kg {
    type: number
    sql: quantity_or_weight_kg ;;
    description: ""
  }

  dimension: cost_amount {
    type: number
    sql: cost_amount ;;
    description: ""
  }

  dimension: total_value {
    type: number
    sql: total_value ;;
    description: ""
  }
}
'''

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            "--target-dir", 'tests/fixtures',
            "--output-dir", 'output/tests/',
            "--select", 'example_retail_data__fact_daily_sales',
            "--use-table-name",
            "--skip-explore-joins",
        ])
        self._extracted_from_test_integration_skip_251(
            cli,
            args,
            'output/tests/example/retail_data/fact_daily_sales_v1.view.lkml',
            expected_content,
        )
        
    def test_integration_skip_explore_joins(self):
        
        expected_content = '''
view: example_retail_data__fact_daily_sales {
  label: "Example Retail Data  Fact Daily Sales"
  sql_table_name: `example-project-123`.`retail_data`.`fact_daily_sales_v1` ;;

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

  dimension: dim_store_key {
    type: number
    sql: ${TABLE}.dim_store_key ;;
    description: ""
  }

  dimension: dim_product_key {
    type: number
    sql: ${TABLE}.dim_product_key ;;
    description: ""
  }

  dimension: dim_store_product_key {
    type: number
    sql: ${TABLE}.dim_store_product_key ;;
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

view: example_retail_data__fact_daily_sales__sales {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_payment_method_key {
    type: number
    sql: dim_payment_method_key ;;
    description: ""
  }

  dimension: transaction_type_code {
    type: string
    sql: transaction_type_code ;;
    description: ""
  }

  dimension: promotion_type_id {
    type: string
    sql: promotion_type_id ;;
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

  dimension: sales_amount {
    type: number
    sql: sales_amount ;;
    description: ""
  }

  dimension: profit_amount {
    type: number
    sql: profit_amount ;;
    description: ""
  }

  dimension: fact_transaction_keys {
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
    hidden: yes
    tags: ["array"]
  }

  dimension: fact_transaction_keys_sketch {
    type: string
    sql: fact_transaction_keys_sketch ;;
    description: "HLL++-sketch to efficiently approximate number of visits."
  }
}

view: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: fact_transaction_keys {
    type: number
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
  }
}

view: example_retail_data__fact_daily_sales__waste {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_waste_reason_key {
    type: number
    sql: dim_waste_reason_key ;;
    description: ""
  }

  dimension: quantity_or_weight_kg {
    type: number
    sql: quantity_or_weight_kg ;;
    description: ""
  }

  dimension: cost_amount {
    type: number
    sql: cost_amount ;;
    description: ""
  }

  dimension: total_value {
    type: number
    sql: total_value ;;
    description: ""
  }
}
'''

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            "--target-dir", 'tests/fixtures',
            "--output-dir", 'output/tests/',
            "--select", 'example_retail_data__fact_daily_sales',
            "--skip-explore-joins",
        ])
        self._extracted_from_test_integration_skip_251(
            cli,
            args,
            'output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml',
            expected_content,
        )


    def test_integration_skip(self):
        
        expected_content = '''
view: example_retail_data__fact_daily_sales {
  label: "Example Retail Data  Fact Daily Sales"
  sql_table_name: `example-project-123`.`retail_data`.`fact_daily_sales_v1` ;;

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

  dimension: dim_store_key {
    type: number
    sql: ${TABLE}.dim_store_key ;;
    description: ""
  }

  dimension: dim_product_key {
    type: number
    sql: ${TABLE}.dim_product_key ;;
    description: ""
  }

  dimension: dim_store_product_key {
    type: number
    sql: ${TABLE}.dim_store_product_key ;;
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

view: example_retail_data__fact_daily_sales__sales {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_payment_method_key {
    type: number
    sql: dim_payment_method_key ;;
    description: ""
  }

  dimension: transaction_type_code {
    type: string
    sql: transaction_type_code ;;
    description: ""
  }

  dimension: promotion_type_id {
    type: string
    sql: promotion_type_id ;;
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

  dimension: sales_amount {
    type: number
    sql: sales_amount ;;
    description: ""
  }

  dimension: profit_amount {
    type: number
    sql: profit_amount ;;
    description: ""
  }

  dimension: fact_transaction_keys {
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
    hidden: yes
    tags: ["array"]
  }

  dimension: fact_transaction_keys_sketch {
    type: string
    sql: fact_transaction_keys_sketch ;;
    description: "HLL++-sketch to efficiently approximate number of visits."
  }
}

view: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: fact_transaction_keys {
    type: number
    sql: fact_transaction_keys ;;
    description: "Array of salted keys for fact_transaction_key on receipt-line-level. Used to calculate unique number of visits."
  }
}

view: example_retail_data__fact_daily_sales__waste {
  label: "Example Retail Data  Fact Daily Sales"

  dimension: dim_waste_reason_key {
    type: number
    sql: dim_waste_reason_key ;;
    description: ""
  }

  dimension: quantity_or_weight_kg {
    type: number
    sql: quantity_or_weight_kg ;;
    description: ""
  }

  dimension: cost_amount {
    type: number
    sql: cost_amount ;;
    description: ""
  }

  dimension: total_value {
    type: number
    sql: total_value ;;
    description: ""
  }
}

explore: example_retail_data__fact_daily_sales {
  label: "Example Retail Data  Fact Daily Sales"
  from: example_retail_data__fact_daily_sales

  join: example_retail_data__fact_daily_sales__sales {
    relationship: one_to_many
    sql: LEFT JOIN UNNEST(${example_retail_data__fact_daily_sales.sales}) AS example_retail_data__fact_daily_sales__sales ;;
    type: left_outer
    required_joins: []
  }

  join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {
    relationship: one_to_many
    sql: LEFT JOIN UNNEST(${example_retail_data__fact_daily_sales.sales.fact_transaction_keys}) AS example_retail_data__fact_daily_sales__sales__fact_transaction_keys ;;
    type: left_outer
    required_joins: []
  }

  join: example_retail_data__fact_daily_sales__waste {
    relationship: one_to_many
    sql: LEFT JOIN UNNEST(${example_retail_data__fact_daily_sales.waste}) AS example_retail_data__fact_daily_sales__waste ;;
    type: left_outer
    required_joins: []
  }

  hidden: no
}
'''

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args([
            "--target-dir", 'tests/fixtures',
            "--output-dir", 'output/tests/',
            "--select", 'example_retail_data__fact_daily_sales',
        ])
        self._extracted_from_test_integration_skip_251(
            cli,
            args,
            'output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml',
            expected_content,
        )

    # TODO Rename this here and in `test_integration_skip_explore_joins_and_use_table_name`, `test_integration_skip_explore_joins` and `test_integration_skip`
    def _extracted_from_test_integration_skip_251(self, cli, args, arg2, expected_content):
        cli.generate(args)
        assert os.path.exists(arg2)
        with open(arg2) as f:
            content = f.read()
        assert content.strip() == expected_content.strip()



    