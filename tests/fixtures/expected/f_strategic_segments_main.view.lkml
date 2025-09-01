explore: f_strategic_segments_main {
  hidden: yes
    join: f_strategic_segments_main__float_input {
      view_label: "F Strategic Segments Main: Float Input"
      sql: LEFT JOIN UNNEST(${f_strategic_segments_main.float_input}) as f_strategic_segments_main__float_input ;;
      relationship: one_to_many
    }
}
view: f_strategic_segments_main {
  sql_table_name: `ac16-p-conlaybi-prd-4257.consumer.f_strategic_segments_main` ;;
 
  dimension: d_bonus_account_key {
    type: number
    description: "Main identifier. Level that is being segmented"
    sql: ${TABLE}.d_bonus_account_key ;;
  }
  dimension: float_input {
    hidden: yes
    sql: ${TABLE}.float_input ;;
  }
  dimension: food_passion {
    type: number
    description: "Feature input for Food passion"
    sql: ${TABLE}.food_passion ;;
  }
  dimension: price_sensitivity {
    type: number
    description: "Feature input for Price sensitivity"
    sql: ${TABLE}.price_sensitivity ;;
  }
  dimension_group: run {
    type: time
    description: "Date of model run"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.run_date ;;
  }
  dimension_group: run_month {
    type: time
    description: "First day of month of model run"
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.run_month ;;
  }
  dimension: segment {
    type: number
    description: "Segment category key for each bonus account"
    sql: ${TABLE}.segment ;;
  }
  dimension: share_of_eco {
    type: number
    description: "Feature input for Eco"
    sql: ${TABLE}.share_of_eco ;;
  }
  dimension: share_of_vego {
    type: number
    description: "Feature input for Vego"
    sql: ${TABLE}.share_of_vego ;;
  }
  measure: count {
    type: count
  }
}
 
view: f_strategic_segments_main__float_input {
 
  dimension: f_strategic_segments_main__float_input {
    type: number
    description: "Array of normalized feature input. Food passion, Price sensitivity, Eco demand and Vego demand"
    sql: f_strategic_segments_main__float_input ;;
  }
}