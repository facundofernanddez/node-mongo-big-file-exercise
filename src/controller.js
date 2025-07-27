const Records = require("./records.model");
const fs = require("fs");
const csv = require("csv");

const BATCH_SIZE = 1000; //Tamaño del lote (ajustable segun memoria y db)

/*
Subir y procesar un CSV grande de manera eficiente, parsea linea por linea y guarda en db por lotes (menos uso de memoria RAM y escalable a archivos grandes)
*/
const upload = async (req, res) => {
  const { file } = req;

  let batch = []; // acumulador de filas.
  let totalRows = 0; // contador de filas procesadas.

  const stream = fs
    .createReadStream(file.path)
    .pipe(csv.parse({ trim: true, skip_empty_lines: true, columns: true }));

  stream.on("data", async (row) => {
    batch.push(row);

    //Si llegamos al tamaño del lote, guardamos en DB y vaciamos el lote.
    if (batch.length >= BATCH_SIZE) {
      stream.pause(); // pausamos stream mientras guardamos en db.

      try {
        await Records.insertMany(batch);

        totalRows += batch.length;
        console.log(`guardadas ${batch.length} filas Total: ${totalRows}`);

        batch = []; // limpiamos lote
      } catch (error) {
        console.error("Error al insertar lote", error);
      } finally {
        stream.resume(); // reanudamos stream
      }
    }
  });

  stream.on("end", async () => {
    //insertamos lo ultimo que haya quedado en el lote.
    if (batch.length > 0) {
      try {
        await Records.insertMany(batch);
        totalRows += batch.length;
        console.log(
          `guardadas ultimas ${batch.length} filas. Total: ${totalRows}`
        );
      } catch (error) {
        console.error("Error al insertal lote final");
      }
    }

    console.log(
      `Procesamiento completo. Total de filas guardadas ${totalRows}`
    );
  });

  return res.status(200).json({ message: "Archivo recibido, procesándolo..." });
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
