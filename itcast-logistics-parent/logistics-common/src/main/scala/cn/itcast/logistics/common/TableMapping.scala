package cn.itcast.logistics.common

/**
 * 定义表名：根据表的名字定义属性
 */
object TableMapping {
	
	// Logistics 物流系统表名称
	val AREAS: String = "tbl_areas"
	val CHARGE_STANDARD: String = "tbl_charge_standard"
	val CODES: String = "tbl_codes"
	val COLLECT_PACKAGE: String = "tbl_collect_package"
	val COMPANY: String = "tbl_company"
	val COMPANY_DOT_MAP: String = "tbl_company_dot_map"
	val COMPANY_TRANSPORT_ROUTE_MA: String = "tbl_company_transport_route_ma"
	val COMPANY_WAREHOUSE_MAP: String = "tbl_company_warehouse_map"
	val CONSUMER_SENDER_INFO: String = "tbl_consumer_sender_info"
	val COURIER: String = "tbl_courier"
	val DELIVER_PACKAGE: String = "tbl_deliver_package"
	val DELIVER_REGION: String = "tbl_deliver_region"
	val DELIVERY_RECORD: String = "tbl_delivery_record"
	val DEPARTMENT: String = "tbl_department"
	val DOT: String = "tbl_dot"
	val DOT_TRANSPORT_TOOL: String = "tbl_dot_transport_tool"
	val DRIVER: String = "tbl_driver"
	val EMP: String = "tbl_emp"
	val EMP_INFO_MAP: String = "tbl_emp_info_map"
	val EXPRESS_BILL: String = "tbl_express_bill"
	val EXPRESS_PACKAGE: String = "tbl_express_package"
	val FIXED_AREA: String = "tbl_fixed_area"
	val GOODS_RACK: String = "tbl_goods_rack"
	val JOB: String = "tbl_job"
	val OUT_WAREHOUSE: String = "tbl_out_warehouse"
	val OUT_WAREHOUSE_DETAIL: String = "tbl_out_warehouse_detail"
	val PKG: String = "tbl_pkg"
	val POSTAL_STANDARD: String = "tbl_postal_standard"
	val PUSH_WAREHOUSE: String = "tbl_push_warehouse"
	val PUSH_WAREHOUSE_DETAIL: String = "tbl_push_warehouse_detail"
	val ROUTE: String = "tbl_route"
	val SERVICE_EVALUATION: String = "tbl_service_evaluation"
	val STORE_GRID: String = "tbl_store_grid"
	val TRANSPORT_RECORD: String = "tbl_transport_record"
	val TRANSPORT_TOOL: String = "tbl_transport_tool"
	val VEHICLE_MONITOR: String = "tbl_vehicle_monitor"
	
	val WAREHOUSE: String = "tbl_warehouse"
	val WAREHOUSE_EMP: String = "tbl_warehouse_emp"
	val WAREHOUSE_RACK_MAP: String = "tbl_warehouse_rack_map"
	val WAREHOUSE_RECEIPT: String = "tbl_warehouse_receipt"
	val WAREHOUSE_RECEIPT_DETAIL: String = "tbl_warehouse_receipt_detail"
	val WAREHOUSE_SEND_VEHICLE: String = "tbl_warehouse_send_vehicle"
	val WAREHOUSE_TRANSPORT_TOOL: String = "tbl_warehouse_transport_tool"
	val WAREHOUSE_VEHICLE_MAP: String = "tbl_warehouse_vehicle_map"
	val WAY_BILL: String = "tbl_waybill"
	val WAYBILL_LINE: String = "tbl_waybill_line"
	val WAYBILL_STATE_RECORD: String = "tbl_waybill_state_record"
	val WORK_TIME: String = "tbl_work_time"
	
	// CRM 系统业务数据表名称
	val ADDRESS: String = "tbl_address"
	val CONSUMER_ADDRESS_MAP: String = "tbl_consumer_address_map"
	val CUSTOMER: String = "tbl_customer"
}
