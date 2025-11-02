use arrow::array::{
    Array, ArrayRef, BooleanArray, ListArray, StringArray, StringBuilder, StructArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use netdev::Interface;
use std::sync::Arc;

pub fn ifaces2batch(interfaces: &[Interface]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("friendly_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("if_type", DataType::Utf8, false),
        Field::new("mac_addr", DataType::Utf8, true),
        Field::new(
            "ipv4",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("addr", DataType::Utf8, false),
                        Field::new("prefix_len", DataType::UInt8, false),
                    ]
                    .into(),
                ),
                true,
            ))),
            false,
        ),
        Field::new(
            "ipv6",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("addr", DataType::Utf8, false),
                        Field::new("prefix_len", DataType::UInt8, false),
                    ]
                    .into(),
                ),
                true,
            ))),
            false,
        ),
        Field::new("flags", DataType::UInt32, false),
        Field::new("oper_state", DataType::Utf8, false),
        Field::new("transmit_speed", DataType::UInt64, true),
        Field::new("receive_speed", DataType::UInt64, true),
        Field::new(
            "stats",
            DataType::Struct(
                vec![
                    Field::new("rx_bytes", DataType::UInt64, false),
                    Field::new("tx_bytes", DataType::UInt64, false),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "gateway",
            DataType::Struct(
                vec![
                    Field::new("mac_addr", DataType::Utf8, false),
                    Field::new(
                        "ipv4",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        false,
                    ),
                    Field::new(
                        "ipv6",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        false,
                    ),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "dns_servers",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("mtu", DataType::UInt32, true),
        Field::new("default", DataType::Boolean, false),
        Field::new("is_up", DataType::Boolean, false),
        Field::new("is_loopback", DataType::Boolean, false),
        Field::new("is_multicast", DataType::Boolean, false),
        Field::new("is_broadcast", DataType::Boolean, false),
        Field::new("is_point_to_point", DataType::Boolean, false),
        Field::new("is_tun", DataType::Boolean, false),
        Field::new("is_running", DataType::Boolean, false),
        Field::new("is_physical", DataType::Boolean, false),
    ]));

    let columns = to_columns(interfaces, &schema)?;
    RecordBatch::try_new(schema, columns)
}

fn to_columns(
    interfaces: &[Interface],
    schema: &Schema,
) -> Result<Vec<ArrayRef>, arrow::error::ArrowError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(UInt32Array::from_iter_values(
        interfaces.iter().map(|iface| iface.index),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        interfaces.iter().map(|iface| &iface.name),
    )));
    columns.push(Arc::new(StringArray::from_iter(
        interfaces
            .iter()
            .map(|iface| iface.friendly_name.as_deref()),
    )));
    columns.push(Arc::new(StringArray::from_iter(
        interfaces.iter().map(|iface| iface.description.as_deref()),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        interfaces
            .iter()
            .map(|iface| format!("{:?}", iface.if_type)),
    )));
    columns.push(Arc::new(StringArray::from_iter(
        interfaces
            .iter()
            .map(|iface| iface.mac_addr.map(|mac| mac.to_string())),
    )));
    columns.push(ipv4_to_arrow(interfaces)?);
    columns.push(ipv6_to_arrow(interfaces)?);
    columns.push(Arc::new(UInt32Array::from_iter_values(
        interfaces.iter().map(|iface| iface.flags),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        interfaces
            .iter()
            .map(|iface| format!("{:?}", iface.oper_state)),
    )));
    columns.push(Arc::new(UInt64Array::from_iter(
        interfaces.iter().map(|iface| iface.transmit_speed),
    )));
    columns.push(Arc::new(UInt64Array::from_iter(
        interfaces.iter().map(|iface| iface.receive_speed),
    )));
    columns.push(stats_to_arrow(interfaces)?);
    columns.push(gateway_to_arrow(interfaces)?);
    columns.push(dns_servers_to_arrow(interfaces)?);
    columns.push(Arc::new(UInt32Array::from_iter(
        interfaces.iter().map(|iface| iface.mtu),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.default)),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_up())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_loopback())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_multicast())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_broadcast())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces
            .iter()
            .map(|iface| Some(iface.is_point_to_point())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_tun())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_running())),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        interfaces.iter().map(|iface| Some(iface.is_physical())),
    )));
    Ok(columns)
}

fn ipv4_to_arrow(interfaces: &[Interface]) -> Result<ArrayRef, arrow::error::ArrowError> {
    let mut offsets = vec![0];
    let mut addrs = StringBuilder::new();
    let mut prefix_lens = UInt8Array::builder(0);

    for iface in interfaces {
        for ipv4 in &iface.ipv4 {
            addrs.append_value(ipv4.addr().to_string());
            prefix_lens.append_value(ipv4.prefix_len());
        }
        let last_offset = offsets[offsets.len() - 1];
        offsets.push(iface.ipv4.len() as i32 + last_offset);
    }

    let addr_array = Arc::new(addrs.finish());
    let prefix_len_array = Arc::new(prefix_lens.finish());

    let fields = Fields::from(vec![
        Field::new("addr", DataType::Utf8, false),
        Field::new("prefix_len", DataType::UInt8, false),
    ]);

    let struct_array = StructArray::try_new(fields, vec![addr_array, prefix_len_array], None)?;

    let list_array = ListArray::try_new(
        Arc::new(Field::new("item", struct_array.data_type().clone(), true)),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        None,
    )?;

    Ok(Arc::new(list_array))
}

fn ipv6_to_arrow(interfaces: &[Interface]) -> Result<ArrayRef, arrow::error::ArrowError> {
    let mut offsets = vec![0];
    let mut addrs = StringBuilder::new();
    let mut prefix_lens = UInt8Array::builder(0);

    for iface in interfaces {
        for ipv6 in &iface.ipv6 {
            addrs.append_value(ipv6.addr().to_string());
            prefix_lens.append_value(ipv6.prefix_len());
        }
        let last_offset = offsets[offsets.len() - 1];
        offsets.push(iface.ipv6.len() as i32 + last_offset);
    }

    let addr_array = Arc::new(addrs.finish());
    let prefix_len_array = Arc::new(prefix_lens.finish());

    let fields = Fields::from(vec![
        Field::new("addr", DataType::Utf8, false),
        Field::new("prefix_len", DataType::UInt8, false),
    ]);

    let struct_array = StructArray::try_new(fields, vec![addr_array, prefix_len_array], None)?;

    let list_array = ListArray::try_new(
        Arc::new(Field::new("item", struct_array.data_type().clone(), true)),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        None,
    )?;

    Ok(Arc::new(list_array))
}

fn stats_to_arrow(interfaces: &[Interface]) -> Result<ArrayRef, arrow::error::ArrowError> {
    let mut rx_bytes = UInt64Array::builder(interfaces.len());
    let mut tx_bytes = UInt64Array::builder(interfaces.len());
    let mut nulls = Vec::with_capacity(interfaces.len());

    for iface in interfaces {
        if let Some(stats) = &iface.stats {
            rx_bytes.append_value(stats.rx_bytes);
            tx_bytes.append_value(stats.tx_bytes);
            nulls.push(true);
        } else {
            rx_bytes.append_null();
            tx_bytes.append_null();
            nulls.push(false);
        }
    }

    let rx_bytes_array = Arc::new(rx_bytes.finish());
    let tx_bytes_array = Arc::new(tx_bytes.finish());

    let fields = Fields::from(vec![
        Field::new("rx_bytes", DataType::UInt64, false),
        Field::new("tx_bytes", DataType::UInt64, false),
    ]);

    let struct_array = StructArray::try_new(
        fields,
        vec![rx_bytes_array, tx_bytes_array],
        Some(arrow::buffer::NullBuffer::new(
            arrow::buffer::BooleanBuffer::from_iter(nulls),
        )),
    )?;

    Ok(Arc::new(struct_array))
}

fn gateway_to_arrow(interfaces: &[Interface]) -> Result<ArrayRef, arrow::error::ArrowError> {
    let mut mac_addrs = StringBuilder::new();
    let mut ipv4_offsets = vec![0];
    let mut ipv4_values = StringBuilder::new();
    let mut ipv6_offsets = vec![0];
    let mut ipv6_values = StringBuilder::new();
    let mut nulls = Vec::with_capacity(interfaces.len());

    for iface in interfaces {
        if let Some(gateway) = &iface.gateway {
            mac_addrs.append_value(gateway.mac_addr.to_string());
            for ip in &gateway.ipv4 {
                ipv4_values.append_value(ip.to_string());
            }
            let last_ipv4_offset = ipv4_offsets[ipv4_offsets.len() - 1];
            ipv4_offsets.push(gateway.ipv4.len() as i32 + last_ipv4_offset);
            for ip in &gateway.ipv6 {
                ipv6_values.append_value(ip.to_string());
            }
            let last_ipv6_offset = ipv6_offsets[ipv6_offsets.len() - 1];
            ipv6_offsets.push(gateway.ipv6.len() as i32 + last_ipv6_offset);
            nulls.push(true);
        } else {
            mac_addrs.append_null();
            let last_ipv4_offset = ipv4_offsets[ipv4_offsets.len() - 1];
            ipv4_offsets.push(last_ipv4_offset);
            let last_ipv6_offset = ipv6_offsets[ipv6_offsets.len() - 1];
            ipv6_offsets.push(last_ipv6_offset);
            nulls.push(false);
        }
    }

    let mac_addrs_array = Arc::new(mac_addrs.finish());
    let ipv4_values_array = Arc::new(ipv4_values.finish());
    let ipv6_values_array = Arc::new(ipv6_values.finish());

    let ipv4_list_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        arrow::buffer::OffsetBuffer::new(ipv4_offsets.into()),
        ipv4_values_array,
        None,
    )?;

    let ipv6_list_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        arrow::buffer::OffsetBuffer::new(ipv6_offsets.into()),
        ipv6_values_array,
        None,
    )?;

    let fields = Fields::from(vec![
        Field::new("mac_addr", DataType::Utf8, false),
        Field::new("ipv4", ipv4_list_array.data_type().clone(), false),
        Field::new("ipv6", ipv6_list_array.data_type().clone(), false),
    ]);

    let struct_array = StructArray::try_new(
        fields,
        vec![
            mac_addrs_array,
            Arc::new(ipv4_list_array),
            Arc::new(ipv6_list_array),
        ],
        Some(arrow::buffer::NullBuffer::new(
            arrow::buffer::BooleanBuffer::from_iter(nulls),
        )),
    )?;

    Ok(Arc::new(struct_array))
}

fn dns_servers_to_arrow(interfaces: &[Interface]) -> Result<ArrayRef, arrow::error::ArrowError> {
    let mut offsets = vec![0];
    let mut values = StringBuilder::new();

    for iface in interfaces {
        for ip in &iface.dns_servers {
            values.append_value(ip.to_string());
        }
        let last_offset = offsets[offsets.len() - 1];
        offsets.push(iface.dns_servers.len() as i32 + last_offset);
    }

    let values_array = Arc::new(values.finish());

    let list_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values_array,
        None,
    )?;

    Ok(Arc::new(list_array))
}
