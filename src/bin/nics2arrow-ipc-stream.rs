use arrow::ipc::writer::StreamWriter;
use rs_nics2arrow_ipc_stream::ifaces2batch;
use std::io::stdout;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let interfaces = netdev::get_interfaces();
    let batch = ifaces2batch(&interfaces)?;
    let mut writer = StreamWriter::try_new(stdout(), &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}
