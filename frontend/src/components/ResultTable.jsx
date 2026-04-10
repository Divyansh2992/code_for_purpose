export default function ResultTable({ result, columns }) {
  if (!result?.length || !columns?.length) return null;

  const display = result.slice(0, 100);

  return (
    <div>
      <div className="result-table-wrap">
        <table className="result-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {display.map((row, i) => (
              <tr key={i}>
                {columns.map((col) => {
                  const val = row[col];
                  const isNull = val === null || val === undefined;
                  return (
                    <td key={col} className={isNull ? 'is-null' : ''}>
                      {isNull ? 'null' : typeof val === 'number' ? val.toLocaleString() : String(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="result-count">
        Showing {display.length} of {result.length} row{result.length !== 1 ? 's' : ''}
      </p>
    </div>
  );
}
