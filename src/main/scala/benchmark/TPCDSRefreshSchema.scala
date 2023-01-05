package benchmark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) extends Serializable {
  val schema = StructType(fields)
}

object TPCDSRefreshSchema {
  val map_view_to_dest: Map[String, String] = Map(
    "crv" -> "catalog_returns",
    "csv" -> "catalog_sales",
    "iv" -> "inventory",
    "srv" -> "store_returns",
    "ssv" -> "store_sales",
    "wrv" -> "web_returns",
    "wsv" -> "web_sales",
  )

  def tables(sqlContext: SQLContext): Seq[Table] = {
    import sqlContext.implicits._

    Seq(
      // Column,Datatype,NULLs,Foreign Key
      // zipg_zip,char(5),N,
      // zipg_gmt_offset,integer,N,
      Table("s_zip_to_gmt",
        partitionColumns = Nil,
        StructField("zipg_zip", StringType, nullable = false),
        StructField("zipg_gmt_offset", LongType, nullable = false),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // plin_purchase_id,identifier,N,
      // plin_line_number,integer,N,
      // plin_item_id,char(16),,i_item_id
      // plin_promotion_id,char(16),,p_promo_id
      // plin_quantity,integer,,
      // plin_sale_price,"numeric(7,2)",,
      // plin_coupon_amt,"numeric(7,2)",,
      // plin_comment,char(100,,
      Table("s_purchase_lineitem",
        partitionColumns = Nil,
        StructField("plin_purchase_id", LongType, nullable = false),
        StructField("plin_line_number", LongType, nullable = false),
        'plin_item_id.string,
        'plin_promotion_id.string,
        'plin_quantity.long,
        'plin_sale_price.decimal(7, 2),
        'plin_coupon_amt.decimal(7, 2),
        'plin_comment.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // cust_customer_id,identifier,N,c_customer_id
      // cust_salutation,char(10),,
      // cust_last_name,char(20),,
      // cust_first_name,char(20),,
      // cust_preffered_flag,char(1),,
      // cust_birth_date,char(10),,
      // cust_birth_country,char(20),,
      // cust_login_id,char(13),,
      // cust_email_address,char(50),,
      // cust_last_login_chg_date,char(10),,
      // cust_first_shipto_date,char(10),,
      // cust_first_purchase_date,char(10),,
      // cust_last_review_date,char(10),,
      // cust_primary_machine_id,char(15),,
      // cust_secondary_machine_id,char(15),,
      // cust_street_number,"char(10),",,
      // cust_suite_number,char(10),,
      // cust_street_name1,char(30),,
      // cust_street_name2,char(30),,
      // cust_street_type,char(15),,
      // cust_city,char(60),,
      // cust_zip,char(10),,
      // cust_county,char(30),,
      // cust_state,char(2),,
      // cust_country,char(20),,
      // cust_loc_type,char(20),,
      // cust_gender,char(1),,cd_gender
      // cust_marital_status,char(1),,cd_marital_status
      // cust_educ_status,char(20),,cd_education_status
      // cust_credit_rating,char(10),,cd_credit_rating
      // cust_purch_est,"numeric(7,2)",,cd_purchase_estimate
      // cust_buy_potential,char(15),,hd_buy_potential
      // cust_depend_cnt,integer,,cd_dep_count
      // cust_depend_emp_cnt,integer,,cd_dep_employed_count
      // cust_depend_college_cnt,integer,,cd_dep_college_count
      // cust_vehicle_cnt,integer,,hd_vehicle_count
      // cust_annual_income,"numeric(9,2)",,"ib_lower_bound, ib_upper_bound"
      Table("s_customer",
        partitionColumns = Nil,
        StructField("cust_customer_id", LongType, nullable = false),
        'cust_salutation.string,
        'cust_last_name.string,
        'cust_first_name.string,
        'cust_preffered_flag.string,
        'cust_birth_date.string,
        'cust_birth_country.string,
        'cust_login_id.string,
        'cust_email_address.string,
        'cust_last_login_chg_date.string,
        'cust_first_shipto_date.string,
        'cust_first_purchase_date.string,
        'cust_last_review_date.string,
        'cust_primary_machine_id.string,
        'cust_secondary_machine_id.string,
        'cust_street_number.string,
        'cust_suite_number.string,
        'cust_street_name1.string,
        'cust_street_name2.string,
        'cust_street_type.string,
        'cust_city.string,
        'cust_zip.string,
        'cust_county.string,
        'cust_state.string,
        'cust_country.string,
        'cust_loc_type.string,
        'cust_gender.string,
        'cust_marital_status.string,
        'cust_educ_status.string,
        'cust_credit_rating.string,
        'cust_purch_est.decimal(7, 2),
        'cust_buy_potential.string,
        'cust_depend_cnt.long,
        'cust_depend_emp_cnt.long,
        'cust_depend_college_cnt.long,
        'cust_vehicle_cnt.long,
        'cust_annual_income.decimal(9, 2),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // purc_purchase_id,identifier,N,plin_purchase_id
      // purc_store_id,char(16),,s_store_id
      // purc_customer_id,char(16),,s_customer_id
      // purc_purchase_date,char(10),,d_date
      // purc_purchase_time,integer,,t_time
      // purc_register_id,integer,,
      // purc_clerk_id,integer,,
      // purc_comment,char(100),,
      Table("s_purchase",
        partitionColumns = Nil,
        StructField("purc_purchase_id", LongType, nullable = false),
        'purc_store_id.string,
        'purc_customer_id.string,
        'purc_purchase_date.string,
        'purc_purchase_time.long,
        'purc_register_id.long,
        'purc_clerk_id.long,
        'purc_comment.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // cord_order_id,identifier,N,clin_order_id
      // cord_bill_customer_id,char(16),,c_customer_id
      // cord_ship_customer_id,char(16),,c_customer_id
      // cord_order_date,char(10),,d_date
      // cord_order_time,integer,,t_time
      // cord_ship_mode_id,char(16),,sm_ship_mode_id
      // cord_call_center_id,char(16),,cc_call_center_id
      // cord_order_comments,varchar(100),,
      Table("s_catalog_order",
        partitionColumns = Nil,
        StructField("cord_order_id", LongType, nullable = false),
        'cord_bill_customer_id.string,
        'cord_ship_customer_id.string,
        'cord_order_date.string,
        'cord_order_time.long,
        'cord_ship_mode_id.string,
        'cord_call_center_id.string,
        'cord_order_comments.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // word_order_id,identifier,N,wlin_order_id
      // word_bill_customer_id,char(16),,c_customer_id
      // word_ship_customer_id,char(16),,c_customer_id
      // word_order_date,char(10),,d_date
      // word_order_time,integer,,t_time
      // word_ship_mode_id,char(16),,sm_ship_mode_id
      // word_web_site_id,char(16),,web_site_id
      // word_order_comments,char(100),,
      Table("s_web_order",
        partitionColumns = Nil,
        StructField("word_order_id", LongType, nullable = false),
        'word_bill_customer_id.string,
        'word_ship_customer_id.string,
        'word_order_date.string,
        'word_order_time.long,
        'word_ship_mode_id.string,
        'word_web_site_id.string,
        'word_order_comments.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // item_item_id,char(16),N,i_item_id
      // item_item_description,char(200),,
      // item_list_price,"numeric(7,2)",,
      // item_wholesale_cost,"numeric(7,2)",,
      // item_size,char(20),,
      // item_formulation,char(20),,
      // item_color,char(20),,
      // item_units,char(10),,
      // item_container,char(10),,
      // item_manager_id,integer,,
      Table("s_item",
        partitionColumns = Nil,
        StructField("item_item_id", StringType, nullable = false),
        'item_item_description.string,
        'item_list_price.decimal(7, 2),
        'item_wholesale_cost.decimal(7, 2),
        'item_size.string,
        'item_formulation.string,
        'item_color.string,
        'item_units.string,
        'item_container.string,
        'item_manager_id.long,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // clin_order_id,identifier,N,cord_order_id
      // clin_line_number,integer,,
      // clin_item_id,char(16),,i_item_id
      // clin_promotion_id,char(16),,p_promo_id
      // clin_quantity,integer,,
      // clin_sales_price,"numeric(7,2)",,
      // clin_coupon_amt,"numeric(7,2)",,
      // clin_warehouse_id,char(16),,w_warehouse_id
      // clin_ship_date,char(10),,
      // clin_catalog_number,integer,,
      // clin_catalog_page_number,integer,,
      // clin_ship_cost,"numeric(7,2)",,
      Table("s_catalog_order_lineitem",
        partitionColumns = Nil,
        StructField("clin_order_id", LongType, nullable = false),
        'clin_line_number.long,
        'clin_item_id.string,
        'clin_promotion_id.string,
        'clin_quantity.long,
        'clin_sales_price.decimal(7, 2),
        'clin_coupon_amt.decimal(7, 2),
        'clin_warehouse_id.string,
        'clin_ship_date.string,
        'clin_catalog_number.long,
        'clin_catalog_page_number.long,
        'clin_ship_cost.decimal(7, 2),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // wlin_order_id,identifier,N,word_order_id
      // wlin_line_number,integer,N,
      // wlin_item_id,char(16),,i_item_id
      // wlin_promotion_id,char(16),,p_promo_id
      // wlin_quantity,integer,,
      // wlin_sales_price,"numeric(7,2)",,
      // wlin_coupon_amt,"numeric(7,2)",,
      // wlin_warehouse_id,char(16),,w_warehouse_id
      // wlin_ship_date,char(10),,d_date
      // wlin_ship_cost,"numeric(7,2)",,
      // wlin_web_page_id,char(16),,wp_web_page
      Table("s_web_order_lineitem",
        partitionColumns = Nil,
        StructField("wlin_order_id", LongType, nullable = false),
        StructField("wlin_line_number", LongType, nullable = false),
        'wlin_item_id.string,
        'wlin_promotion_id.string,
        'wlin_quantity.long,
        'wlin_sales_price.decimal(7, 2),
        'wlin_coupon_amt.decimal(7, 2),
        'wlin_warehouse_id.string,
        'wlin_ship_date.string,
        'wlin_ship_cost.decimal(7, 2),
        'wlin_web_page_id.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // stor_store_id,char(16),N,s_store_id
      // stor_closed_date,char(10),,d_date
      // stor_name,char(50),,
      // stor_employees,integer,,
      // stor_floor_space,integer,,
      // stor_hours,char(20),,
      // stor_store_manager,char(40),,
      // stor_market_id,integer,,
      // stor_geography_class,char(100),,
      // stor_market_manager,char(40),,
      // stor_tax_percentage,"numeric(5,2)",,
      Table("s_store",
        partitionColumns = Nil,
        StructField("stor_store_id", StringType, nullable = false),
        'stor_closed_date.string,
        'stor_name.string,
        'stor_employees.long,
        'stor_floor_space.long,
        'stor_hours.string,
        'stor_store_manager.string,
        'stor_market_id.long,
        'stor_geography_class.string,
        'stor_market_manager.string,
        'stor_tax_percentage.decimal(5, 2),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // call_center_id,char(16),N,cc_center_id
      // call_open_date,char(10),,d_date
      // call_closed_date,char(10),,d_date
      // call_center_name,char(50),,
      // call_center_class,char(50),,
      // call_center_employees,integer,,
      // call_center_sq_ft,integer,,
      // call_center_hours,char(20),,
      // call_center_manager,char(40),,
      // call_center_tax_percentage,"numeric(7,2)",,
      Table("s_call_center",
        partitionColumns = Nil,
        StructField("call_center_id", StringType, nullable = false),
        'call_open_date.string,
        'call_closed_date.string,
        'call_center_name.string,
        'call_center_class.string,
        'call_center_employees.long,
        'call_center_sq_ft.long,
        'call_center_hours.string,
        'call_center_manager.string,
        'call_center_tax_percentage.decimal(7, 2),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // wsit_web_site_id,char(16),N,web_site_id
      // wsit_open_date,char(10),,d_date
      // wsit_closed_date,char(10),,d_date
      // wsit_site_name,char(50),,
      // wsit_site_class,char(50),,
      // wsit_site_manager,"char(40),",,
      // wsit_tax_percentage,"decimal(5,2)",,
      Table("s_web_site",
        partitionColumns = Nil,
        StructField("wsit_web_site_id", StringType, nullable = false),
        'wsit_open_date.string,
        'wsit_closed_date.string,
        'wsit_site_name.string,
        'wsit_site_class.string,
        'wsit_site_manager.string,
        'wsit_tax_percentage.decimal(5, 2),
      ),
      // Column,Datatype,NULLs,Foreign Key
      // wrhs_warehouse_id,char(16),N,w_warehouse_id
      // wrhs_warehouse_desc,char(200),,
      // wrhs_warehouse_sq_ft,integer,,
      Table("s_warehouse",
        partitionColumns = Nil,
        StructField("wrhs_warehouse_id", StringType, nullable = false),
        'wrhs_warehouse_desc.string,
        'wrhs_warehouse_sq_ft.long,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // wpag_web_page_id,char(16),N,web_page_id
      // wpag_create_date,char(10),,d_date
      // wpag_access_date,char(10),,d_date
      // wpag_autogen_flag,char(1),,
      // wpag_url,char(100),,
      // wpag_type,char(50),,
      // wpag_char_cnt,integer,,
      // wpag_link_cnt,integer,,
      // wpag_image_cnt,integer,,
      // wpag_max_ad_cnt,integer,,
      Table("s_web_page",
        partitionColumns = Nil,
        StructField("wpag_web_page_id", StringType, nullable = false),
        'wpag_create_date.string,
        'wpag_access_date.string,
        'wpag_autogen_flag.string,
        'wpag_url.string,
        'wpag_type.string,
        'wpag_char_cnt.long,
        'wpag_link_cnt.long,
        'wpag_image_cnt.long,
        'wpag_max_ad_cnt.long,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // prom_promotion_id,char(16),N,
      // prom_promotion_name,char(30),,
      // prom_start_date,char(10),,d_date
      // prom_end_date,char(10),,d_date
      // prom_cost,"numeric(7,2)",,
      // prom_response_target,char(1),,
      // prom_channel_dmail,char(1),,
      // prom_channel_email,char(1),,
      // prom_channel_catalog,char(1),,
      // prom_channel_tv,char(1),,
      // prom_channel_radio,char(1),,
      // prom_channel_press,char(1),,
      // prom_channel_event,char(1),,
      // prom_channel_demo,char(1),,
      // prom_channel_details,char(100),,
      // prom_purpose,char(15),,
      // prom_discount_active,char(1),,
      Table("s_promotion",
        partitionColumns = Nil,
        StructField("prom_promotion_id", StringType, nullable = false),
        'prom_promotion_name.string,
        'prom_start_date.string,
        'prom_end_date.string,
        'prom_cost.decimal(7, 2),
        'prom_response_target.string,
        'prom_channel_dmail.string,
        'prom_channel_email.string,
        'prom_channel_catalog.string,
        'prom_channel_tv.string,
        'prom_channel_radio.string,
        'prom_channel_press.string,
        'prom_channel_event.string,
        'prom_channel_demo.string,
        'prom_channel_details.string,
        'prom_purpose.string,
        'prom_discount_active.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // sret_store_id,char(16),,s_store_id
      // sret_purchase_id,char(16),N,
      // sret_line_number,integer,N,
      // sret_item_id,char(16),N,
      // sret_customer_id,char(16),,s_customer_id
      // sret_return_date,char(10),,d_date
      // sret_return_time,char(10),,t_time
      // sret_ticket_number,char(20),,
      // sret_return_qty,integer,,
      // sret_return_amount,"numeric(7,2)",,
      // sret_return_tax,"numeric(7,2)",,
      // sret_return_fee,"numeric(7,2)",,
      // sret_return_ship_cost,"numeric(7,2)",,
      // sret_refunded_cash,"numeric(7,2)",,
      // sret_reversed_charge,"numeric(7,2)",,
      // sret_store_credit,"numeric(7,2)",,
      // sret_reason_id,char(16),,r_reason_id
      Table("s_store_returns",
        partitionColumns = Nil,
        'sret_store_id.string,
        StructField("sret_purchase_id", StringType, nullable = false),
        StructField("sret_line_number", LongType, nullable = false),
        StructField("sret_item_id", StringType, nullable = false),
        'sret_customer_id.string,
        'sret_return_date.string,
        'sret_return_time.string,
        'sret_ticket_number.long,
        'sret_return_qty.long,
        'sret_return_amount.decimal(7, 2),
        'sret_return_tax.decimal(7, 2),
        'sret_return_fee.decimal(7, 2),
        'sret_return_ship_cost.decimal(7, 2),
        'sret_refunded_cash.decimal(7, 2),
        'sret_reversed_charge.decimal(7, 2),
        'sret_store_credit.decimal(7, 2),
        'sret_reason_id.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // cret_call_center_id,char(16),,cc_call_center_id
      // cret_order_id,integer,N,
      // cret_line_number,integer,N,
      // cret_item_id,char(16),N,i_item_id
      // cret_return_customer_id,char(16),,c_customer_id
      // cret_refund_customer_id,char(16),,c_customer_id
      // cret_return_date,char(10),,d_date
      // cret_return_time,char(10),,t_time
      // cret_return_qty,integer,,
      // cret_return_amt,"numeric(7,2)",,
      // cret_return_tax,"numeric(7,2)",,
      // cret_return_fee,"numeric(7,2)",,
      // cret_return_ship_cost,"numeric(7,2)",,
      // cret_refunded_cash,"numeric(7,2)",,
      // cret_reversed_charge,"numeric(7,2)",,
      // cret_merchant_credit,"numeric(7,2)",,
      // cret_reason_id,char(16),,r_reason_id
      // cret_shipmode_id,char(16),,
      // cret_catalog_page_id,char(16),,
      // cret_warehouse_id,char(16),,
      Table("s_catalog_returns",
        partitionColumns = Nil,
        'cret_call_center_id.string,
        StructField("cret_order_id", LongType, nullable = false),
        StructField("cret_line_number", LongType, nullable = false),
        StructField("cret_item_id", StringType, nullable = false),
        'cret_return_customer_id.string,
        'cret_refund_customer_id.string,
        'cret_return_date.string,
        'cret_return_time.string,
        'cret_return_qty.long,
        'cret_return_amt.decimal(7, 2),
        'cret_return_tax.decimal(7, 2),
        'cret_return_fee.decimal(7, 2),
        'cret_return_ship_cost.decimal(7, 2),
        'cret_refunded_cash.decimal(7, 2),
        'cret_reversed_charge.decimal(7, 2),
        'cret_merchant_credit.decimal(7, 2),
        'cret_reason_id.string,
        'cret_shipmode_id.string,
        'cret_catalog_page_id.string,
        'cret_warehouse_id.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // wret_web_page_id,char(16),,wp_web_page_id
      // wret_order_id,integer,N,
      // wret_line_number,integer,N,
      // wret_item_id,char(16),N,i_item_id
      // wret_return_customer_id,char(16),,c_customer_id
      // wret_refund_customer_id,char(16),,c_customer_id
      // wret_return_date,char(10),,d_date
      // wret_return_time,char(10),,t_time
      // wret_return_qty,integer,,
      // wret_return_amt,"numeric(7,2)",,
      // wret_return_tax,"numeric(7,2)",,
      // wret_return_fee,"numeric(7,2)",,
      // wret_return_ship_cost,"numeric(7,2)",,
      // wret_refunded_cash,"numeric(7,2)",,
      // wret_reversed_charge,"numeric(7,2)",,
      // wret_account_credit,"numeric(7,2)",,
      // wret_reason_id,char(16),,r_reason_id
      Table("s_web_returns",
        partitionColumns = Nil,
        'wret_web_page_id.string,
        StructField("wret_order_id", LongType, nullable = false),
        StructField("wret_line_number", LongType, nullable = false),
        StructField("wret_item_id", StringType, nullable = false),
        'wret_return_customer_id.string,
        'wret_refund_customer_id.string,
        'wret_return_date.string,
        'wret_return_time.string,
        'wret_return_qty.long,
        'wret_return_amt.decimal(7, 2),
        'wret_return_tax.decimal(7, 2),
        'wret_return_fee.decimal(7, 2),
        'wret_return_ship_cost.decimal(7, 2),
        'wret_refunded_cash.decimal(7, 2),
        'wret_reversed_charge.decimal(7, 2),
        'wret_account_credit.decimal(7, 2),
        'wret_reason_id.string,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // invn_warehouse_id,"char(16),",N,w_warehouse_id
      // invn_item_id,"char(16),",N,i_item_id
      // invn_date,char(10),N,d_date
      // invn_qty_on_hand,integer,,
      Table("s_inventory",
        partitionColumns = Nil,
        StructField("invn_warehouse_id", StringType, nullable = false),
        StructField("invn_item_id", StringType, nullable = false),
        StructField("invn_date", StringType, nullable = false),
        'invn_qty_on_hand.long,
      ),
      // Column,Datatype,NULLs,Foreign Key
      // cpag_catalog_number,integer,N,
      // cpag_catalog_page_number,integer,N,
      // cpag_department,char(20),,
      // cpag_id,char(16),,
      // cpag_start_date,char(10),,d_date
      // cpag_end_date,char(10),,d_date
      // cpag_description,varchar(100),,
      // cpag_type,varchar(100),,
      Table("s_catalog_page",
        partitionColumns = Nil,
        StructField("cpag_catalog_number", LongType, nullable = false),
        StructField("cpag_catalog_page_number", LongType, nullable = false),
        'cpag_department.string,
        'cpag_id.string,
        'cpag_start_date.string,
        'cpag_end_date.string,
        'cpag_description.string,
        'cpag_type.string,
      ),
    )
  }

  val viewNames = Seq("crv", "csv", "iv", "srv", "ssv", "wrv", "wsv")

  def getViewQuery(viewName: String, revision: Int): String = {
    viewName match {
      case "crv" => crv_view(revision)
      case "csv" => csv_view(revision)
      case "iv" => iv_view(revision)
      case "srv" => srv_view(revision)
      case "ssv" => ssv_view(revision)
      case "wrv" => wrv_view(revision)
      case "wsv" => wsv_view(revision)
    }
  }
  

  private def ssv_view(r: Int) =
    s"""SELECT d_date_sk ss_sold_date_sk,
       |t_time_sk ss_sold_time_sk,
       |i_item_sk ss_item_sk,
       |c_customer_sk ss_customer_sk,
       |c_current_cdemo_sk ss_cdemo_sk,
       |c_current_hdemo_sk ss_hdemo_sk,
       |c_current_addr_sk ss_addr_sk,
       |s_store_sk ss_store_sk,
       |p_promo_sk ss_promo_sk,
       |purc_purchase_id ss_ticket_number,
       |plin_quantity ss_quantity,
       |i_wholesale_cost ss_wholesale_cost,
       |i_current_price ss_list_price,
       |plin_sale_price ss_sales_price,
       |(i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
       |plin_sale_price * plin_quantity ss_ext_sales_price,
       |i_wholesale_cost * plin_quantity ss_ext_wholesale_cost,
       |i_current_price * plin_quantity ss_ext_list_price,
       |i_current_price * s_tax_precentage ss_ext_tax,
       |plin_coupon_amt ss_coupon_amt,
       |(plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
       |((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
       |((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
       |FROM s_purchase
       |LEFT OUTER JOIN customer ON (purc_customer_id = c_customer_id)
       |LEFT OUTER JOIN store ON (purc_store_id = s_store_id)
       |LEFT OUTER JOIN date_dim ON (cast(purc_purchase_date as date) = d_date)
       |LEFT OUTER JOIN time_dim ON (PURC_PURCHASE_TIME = t_time)
       |JOIN s_purchase_lineitem ON (purc_purchase_id = plin_purchase_id)
       |LEFT OUTER JOIN promotion ON plin_promotion_id = p_promo_id
       |LEFT OUTER JOIN item ON plin_item_id = i_item_id
       |WHERE purc_purchase_id = plin_purchase_id
       |AND i_rec_end_date is NULL
       |AND s_rec_end_date is NULL""".stripMargin

  private def srv_view(r: Int) =
    s"""SELECT d_date_sk sr_returned_date_sk
       |,t_time_sk sr_return_time_sk
       |,i_item_sk sr_item_sk
       |,c_customer_sk sr_customer_sk
       |,c_current_cdemo_sk sr_cdemo_sk
       |,c_current_hdemo_sk sr_hdemo_sk
       |,c_current_addr_sk sr_addr_sk
       |,s_store_sk sr_store_sk
       |,r_reason_sk sr_reason_sk
       |,sret_ticket_number sr_ticket_number
       |,sret_return_qty sr_return_quantity
       |,sret_return_amount sr_return_amt
       |,sret_return_tax sr_return_tax
       |,sret_return_amount + sret_return_tax sr_return_amt_inc_tax
       |,sret_return_fee sr_fee
       |,sret_return_ship_cost sr_return_ship_cost
       |,sret_refunded_cash sr_refunded_cash
       |,sret_reversed_charge sr_reversed_charge
       |,sret_store_credit sr_store_credit
       |,sret_return_amount+sret_return_tax+sret_return_fee
       |-sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
       |FROM s_store_returns
       |LEFT OUTER JOIN date_dim
       |ON (cast(sret_return_date as date) = d_date)
       |LEFT OUTER JOIN time_dim
       |ON (( cast(substr(sret_return_time,1,2) AS integer)*3600
       |+cast(substr(sret_return_time,4,2) AS integer)*60
       |+cast(substr(sret_return_time,7,2) AS integer)) = t_time)
       |LEFT OUTER JOIN item ON (sret_item_id = i_item_id)
       |LEFT OUTER JOIN customer ON (sret_customer_id = c_customer_id)
       |LEFT OUTER JOIN store ON (sret_store_id = s_store_id)
       |LEFT OUTER JOIN reason ON (sret_reason_id = r_reason_id)
       |WHERE i_rec_end_date IS NULL
       |AND s_rec_end_date IS NULL""".stripMargin

  private def wsv_view(r: Int) =
    s"""SELECT d1.d_date_sk ws_sold_date_sk,
       |t_time_sk ws_sold_time_sk,
       |d2.d_date_sk ws_ship_date_sk,
       |i_item_sk ws_item_sk,
       |c1.c_customer_sk ws_bill_customer_sk,
       |c1.c_current_cdemo_sk ws_bill_cdemo_sk,
       |c1.c_current_hdemo_sk ws_bill_hdemo_sk,
       |c1.c_current_addr_sk ws_bill_addr_sk,
       |c2.c_customer_sk ws_ship_customer_sk,
       |c2.c_current_cdemo_sk ws_ship_cdemo_sk,
       |c2.c_current_hdemo_sk ws_ship_hdemo_sk,
       |c2.c_current_addr_sk ws_ship_addr_sk,
       |wp_web_page_sk ws_web_page_sk,
       |web_site_sk ws_web_site_sk,
       |sm_ship_mode_sk ws_ship_mode_sk,
       |w_warehouse_sk ws_warehouse_sk,
       |p_promo_sk ws_promo_sk,
       |word_order_id ws_order_number,
       |wlin_quantity ws_quantity,
       |i_wholesale_cost ws_wholesale_cost,
       |i_current_price ws_list_price,
       |wlin_sales_price ws_sales_price,
       |(i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
       |wlin_sales_price * wlin_quantity ws_ext_sales_price,
       |i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost,
       |i_current_price * wlin_quantity ws_ext_list_price,
       |i_current_price * web_tax_percentage ws_ext_tax,
       |wlin_coupon_amt ws_coupon_amt,
       |wlin_ship_cost * wlin_quantity WS_EXT_SHIP_COST,
       |(wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
       |((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
       |((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
       |(wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
       |+ i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
       |((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
       |FROM s_web_order
       |LEFT OUTER JOIN date_dim d1 ON (cast(word_order_date as date) = d1.d_date)
       |LEFT OUTER JOIN time_dim ON (word_order_time = t_time)
       |LEFT OUTER JOIN customer c1 ON (word_bill_customer_id = c1.c_customer_id)
       |LEFT OUTER JOIN customer c2 ON (word_ship_customer_id = c2.c_customer_id)
       |LEFT OUTER JOIN web_site ON (word_web_site_id = web_site_id AND web_rec_end_date IS NULL)
       |LEFT OUTER JOIN ship_mode ON (word_ship_mode_id = sm_ship_mode_id)
       |JOIN s_web_order_lineitem ON (word_order_id = wlin_order_id)
       |LEFT OUTER JOIN date_dim d2 ON (cast(wlin_ship_date as date) = d2.d_date)
       |LEFT OUTER JOIN item ON (wlin_item_id = i_item_id AND i_rec_end_date IS NULL)
       |LEFT OUTER JOIN web_page ON (wlin_web_page_id = wp_web_page_id AND wp_rec_end_date IS NULL)
       |LEFT OUTER JOIN warehouse ON (wlin_warehouse_id = w_warehouse_id)
       |LEFT OUTER JOIN promotion ON (wlin_promotion_id = p_promo_id)""".stripMargin

  private def wrv_view(r: Int) =
    s"""SELECT d_date_sk wr_returned_date_sk
       |,t_time_sk wr_returned_time_sk
       |,i_item_sk wr_item_sk
       |,c1.c_customer_sk wr_refunded_customer_sk
       |,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
       |,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
       |,c1.c_current_addr_sk wr_refunded_addr_sk
       |,c2.c_customer_sk wr_returning_customer_sk
       |,c2.c_current_cdemo_sk wr_returning_cdemo_sk
       |,c2.c_current_hdemo_sk wr_returning_hdemo_sk
       |,c2.c_current_addr_sk wr_returning_addr_sk
       |,wp_web_page_sk wr_web_page_sk
       |,r_reason_sk wr_reason_sk
       |,wret_order_id wr_order_number
       |,wret_return_qty wr_return_quantity
       |,wret_return_amt wr_return_amt
       |,wret_return_tax wr_return_tax
       |,wret_return_amt + wret_return_tax AS wr_return_amt_inc_tax
       |,wret_return_fee wr_fee
       |,wret_return_ship_cost wr_return_ship_cost
       |,wret_refunded_cash wr_refunded_cash
       |,wret_reversed_charge wr_reversed_charge
       |,wret_account_credit wr_account_credit
       |,wret_return_amt+wret_return_tax+wret_return_fee
       |-wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
       |FROM s_web_returns LEFT OUTER JOIN date_dim ON (cast(wret_return_date as date) = d_date)
       |LEFT OUTER JOIN time_dim ON ((CAST(SUBSTR(wret_return_time,1,2) AS integer)*3600
       |+CAST(SUBSTR(wret_return_time,4,2) AS integer)*60+CAST(SUBSTR(wret_return_time,7,2) AS integer))=t_time)
       |LEFT OUTER JOIN item ON (wret_item_id = i_item_id)
       |LEFT OUTER JOIN customer c1 ON (wret_return_customer_id = c1.c_customer_id)
       |LEFT OUTER JOIN customer c2 ON (wret_refund_customer_id = c2.c_customer_id)
       |LEFT OUTER JOIN reason ON (wret_reason_id = r_reason_id)
       |LEFT OUTER JOIN web_page ON (wret_web_page_id = WP_WEB_PAGE_id)
       |WHERE i_rec_end_date IS NULL AND wp_rec_end_date IS NULL""".stripMargin

  private def csv_view(r: Int) =
    s"""SELECT d1.d_date_sk cs_sold_date_sk
       |,t_time_sk cs_sold_time_sk
       |,d2.d_date_sk cs_ship_date_sk
       |,c1.c_customer_sk cs_bill_customer_sk
       |,c1.c_current_cdemo_sk cs_bill_cdemo_sk
       |,c1.c_current_hdemo_sk cs_bill_hdemo_sk
       |,c1.c_current_addr_sk cs_bill_addr_sk
       |,c2.c_customer_sk cs_ship_customer_sk
       |,c2.c_current_cdemo_sk cs_ship_cdemo_sk
       |,c2.c_current_hdemo_sk cs_ship_hdemo_sk
       |,c2.c_current_addr_sk cs_ship_addr_sk
       |,cc_call_center_sk cs_call_center_sk
       |,cp_catalog_page_sk cs_catalog_page_sk
       |,sm_ship_mode_sk cs_ship_mode_sk
       |,w_warehouse_sk cs_warehouse_sk
       |,i_item_sk cs_item_sk
       |,p_promo_sk cs_promo_sk
       |,cord_order_id cs_order_number
       |,clin_quantity cs_quantity
       |,i_wholesale_cost cs_wholesale_cost
       |,i_current_price cs_list_price
       |,clin_sales_price cs_sales_price
       |,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
       |,clin_sales_price * clin_quantity cs_ext_sales_price
       |,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost
       |,i_current_price * clin_quantity CS_EXT_LIST_PRICE
       |,i_current_price * cc_tax_percentage CS_EXT_TAX
       |,clin_coupon_amt cs_coupon_amt
       |,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
       |,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
       |,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
       |,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
       |,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity)
       |+ i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
       |,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
       |FROM s_catalog_order
       |LEFT OUTER JOIN date_dim d1 ON
       |(cast(cord_order_date as date) = d1.d_date)
       |LEFT OUTER JOIN time_dim ON (cord_order_time = t_time)
       |LEFT OUTER JOIN customer c1 ON (cord_bill_customer_id = c1.c_customer_id)
       |LEFT OUTER JOIN customer c2 ON (cord_ship_customer_id = c2.c_customer_id)
       |LEFT OUTER JOIN call_center ON (cord_call_center_id = cc_call_center_id AND cc_rec_end_date IS NULL)
       |LEFT OUTER JOIN ship_mode ON (cord_ship_mode_id = sm_ship_mode_id)
       |JOIN s_catalog_order_lineitem ON (cord_order_id = clin_order_id)
       |LEFT OUTER JOIN date_dim d2 ON
       |(cast(clin_ship_date as date) = d2.d_date)
       |LEFT OUTER JOIN catalog_page ON
       |(clin_catalog_page_number = cp_catalog_page_number and clin_catalog_number = cp_catalog_number)
       |LEFT OUTER JOIN warehouse ON (clin_warehouse_id = w_warehouse_id)
       |LEFT OUTER JOIN item ON (clin_item_id = i_item_id AND i_rec_end_date IS NULL)
       |LEFT OUTER JOIN promotion ON (clin_promotion_id = p_promo_id)""".stripMargin

  private def crv_view(r: Int) =
    s"""SELECT d_date_sk cr_returned_date_sk
       |,t_time_sk cr_returned_time_sk
       |,i_item_sk cr_item_sk
       |,c1.c_customer_sk cr_refunded_customer_sk
       |,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
       |,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
       |,c1.c_current_addr_sk cr_refunded_addr_sk
       |,c2.c_customer_sk cr_returning_customer_sk
       |,c2.c_current_cdemo_sk cr_returning_cdemo_sk
       |,c2.c_current_hdemo_sk cr_returning_hdemo_sk
       |,c2.c_current_addr_sk cr_returning_addr_sk
       |,cc_call_center_sk cr_call_center_sk
       |,cp_catalog_page_sk CR_CATALOG_PAGE_SK
       |,sm_ship_mode_sk CR_SHIP_MODE_SK
       |,w_warehouse_sk CR_WAREHOUSE_SK
       |,r_reason_sk cr_reason_sk
       |,cret_order_id cr_order_number
       |,cret_return_qty cr_return_quantity
       |,cret_return_amt cr_return_amount
       |,cret_return_tax cr_return_tax
       |,cret_return_amt + cret_return_tax AS cr_return_amt_inc_tax
       |,cret_return_fee cr_fee
       |,cret_return_ship_cost cr_return_ship_cost
       |,cret_refunded_cash cr_refunded_cash
       |,cret_reversed_charge cr_reversed_charge
       |,cret_merchant_credit cr_store_credit
       |,cret_return_amt+cret_return_tax+cret_return_fee
       |-cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
       |FROM s_catalog_returns
       |LEFT OUTER JOIN date_dim
       |ON (cast(cret_return_date as date) = d_date)
       |LEFT OUTER JOIN time_dim ON
       |((CAST(substr(cret_return_time,1,2) AS integer)*3600
       |+CAST(substr(cret_return_time,4,2) AS integer)*60
       |+CAST(substr(cret_return_time,7,2) AS integer)) = t_time)
       |LEFT OUTER JOIN item ON (cret_item_id = i_item_id)
       |LEFT OUTER JOIN customer c1 ON (cret_return_customer_id = c1.c_customer_id)
       |LEFT OUTER JOIN customer c2 ON (cret_refund_customer_id = c2.c_customer_id)
       |LEFT OUTER JOIN reason ON (cret_reason_id = r_reason_id)
       |LEFT OUTER JOIN call_center ON (cret_call_center_id = cc_call_center_id)
       |LEFT OUTER JOIN catalog_page ON (cret_catalog_page_id = cp_catalog_page_id)
       |LEFT OUTER JOIN ship_mode ON (cret_shipmode_id = sm_ship_mode_id)
       |LEFT OUTER JOIN warehouse ON (cret_warehouse_id = w_warehouse_id)
       |WHERE i_rec_end_date IS NULL AND cc_rec_end_date IS NULL""".stripMargin

  private def iv_view(r: Int) =
    s"""SELECT d_date_sk inv_date_sk,
       |i_item_sk inv_item_sk,
       |w_warehouse_sk inv_warehouse_sk,
       |invn_qty_on_hand inv_quantity_on_hand
       |FROM s_inventory
       |LEFT OUTER JOIN warehouse ON (invn_warehouse_id=w_warehouse_id)
       |LEFT OUTER JOIN item ON (invn_item_id=i_item_id AND i_rec_end_date IS NULL)
       |LEFT OUTER JOIN date_dim ON (d_date=invn_date)""".stripMargin
}