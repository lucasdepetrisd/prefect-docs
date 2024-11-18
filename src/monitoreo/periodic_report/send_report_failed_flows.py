"send_report.py"

import os
import datetime
import pytz
from typing import Union

import pandas as pd
from bs4 import BeautifulSoup
from jinja2 import Template
from prefect import task, flow

# from consulterscommons.log_tools import PrefectLogger

from dev.MONITOREO_PREFECT.get_prefect_info import get_prefect_url
from dev.MONITOREO_PREFECT.periodic_report.tipo_ejecucion import TipoEjecucion
from consulterscommons.emails_tools import send_email

GITHUB_REPO_URL = "https://github.com/lucasdepetrisd/prefect-test/blob/main/"

@task
def generate_summary_table(failed_flow_runs: pd.DataFrame) -> str:

    failed_flow_runs = failed_flow_runs.copy()

    # Renombrar columnas
    failed_flow_runs = failed_flow_runs.rename(columns={'date': 'Fecha', 'flow_name': 'Nombre del flujo'})

    # Eliminamos la información de la zona horaria y convertimos a solo fechas
    failed_flow_runs['start_time'] = pd.to_datetime(failed_flow_runs['start_time'], utc=True).dt.tz_localize(None)

    # Ordenar por fecha
    failed_flow_runs = failed_flow_runs.sort_values(by='start_time')

    # Verificar si todas las fechas están en el mismo año
    if failed_flow_runs['start_time'].dt.year.nunique() == 1:
        # Show only month and day
        date_format = '%d-%m'
    else:
        # Show year, month, and day
        date_format = '%d-%m-%Y'

    # Convertir las fechas a strings y ordenarlas
    failed_flow_runs['Fecha'] = failed_flow_runs['start_time'].apply(lambda x: x.strftime(date_format))

    # Contamos las fallas por flujo y fecha
    summary_table = failed_flow_runs.groupby(['Nombre del flujo', 'Fecha']).size().unstack(fill_value=0)

    # Ordenar las columnas por fecha
    sorted_columns = sorted(summary_table.columns, key=lambda x: pd.to_datetime(x, format='%d-%m-%Y' if x.count('-') == 2 else '%d-%m'))
    summary_table = summary_table[sorted_columns]

    # Agregamos una columna 'Total' para contar las fallas por flujo
    summary_table['Total'] = summary_table.sum(axis=1)

    # Agregamos una fila 'Total' para contar las fallas por fecha
    summary_table.loc['Total'] = summary_table.sum(axis=0)

    # Función para aplicar el formato condicional
    def highlight_errors(val):
        if val > 3:
            color = '#ff9999' # Light red
        elif val > 2:
            color = '#ffcccc'  # Light red (pink)
        elif val > 1:
            color = '#ffcc99'  # Light orange
        elif val > 0:
            color = '#ffffcc'  # Light yellow
        else:
            color = '#ffffff'  # White
        return f'background-color: {color}'

    padding = f"{'2px' if len(summary_table.columns.to_list()) > 6 else '3px'}"
    ancho_fechas = '50px' if len(summary_table.columns.to_list()) > 6 else '80px'

    # Aplicar formato condicional y estilos a la tabla
    styled_table = (
        summary_table
        .style
        .map(highlight_errors)
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#6dbf6e'), ('color', '#000001'), ('padding', padding), ('border', '1px solid black')]},
            {'selector': 'td', 'props': [('padding', padding), ('border', '1px solid black')]},
            {
                'selector': 'table', 
                'props': [
                    ('width', '100%'), ('margin-left', 'auto'), ('margin-right', 'auto'),
                    ('border-collapse', 'collapse'), ('border-spacing', '2px'), ('border', '1px solid black')
                    ]
            }  # Centering the table and collapsing borders
        ])
        .set_properties(**{'text-align': 'center'})
        .set_properties(subset=['Total'], **{'width': '45px'})  # Increase width of Total column
        .set_properties(subset=[col for col in summary_table.columns.to_list() if isinstance(col, datetime.date)], **{'width': ancho_fechas})  # Set width of date columns
    )

    # Convertir a HTML y reemplazar saltos de línea para la compatibilidad con correos electrónicos
    html_table = styled_table.to_html()
    html_table = html_table.replace('\n', '')

    return html_table


@task
def add_subflows_to_parent_flows(failed_flow_runs: pd.DataFrame) -> pd.DataFrame:
    # Identify subflows (i.e., those which have a parent)
    subflows = failed_flow_runs[failed_flow_runs['parent_flow_run_id'].notnull()]

    # Create a dictionary mapping parent IDs to their subflows
    subflows_dict = subflows.groupby('parent_flow_run_id').apply(
        lambda df: df[['id', 'flow_id', 'flow_name', 'state', 'start_time', 'end_time', 'ui_url']].to_dict('records'),
        include_groups=False
    ).to_dict()

    # Add subflows information to the parent flows
    failed_flow_runs['subflow_runs'] = failed_flow_runs['id'].map(subflows_dict).fillna("").apply(list)

    # Remove subflows from the dataframe
    failed_flow_runs = failed_flow_runs[failed_flow_runs['parent_flow_run_id'].isnull()]

    return failed_flow_runs


@flow
def send_report_failed_flows(
        failed_flow_runs_df: pd.DataFrame,
        destinatarios: Union[str, list[str]],
        fecha_inicial: datetime.datetime,
        fecha_final: datetime.datetime,
        exec_type: TipoEjecucion
    ) -> str:

    exec_messages = {
            TipoEjecucion.DIARIA: 'el último día',
            TipoEjecucion.SEMANAL: 'la última semana',
            TipoEjecucion.MENSUAL: 'el último mes',
            TipoEjecucion.PERSONALIZADA: f'el rango de fechas de {fecha_inicial.strftime("%d/%m/%Y %H:%M:%S")} a {fecha_final.strftime("%d/%m/%Y %H:%M:%S")}'
    }

    header_message = f"Las siguientes ejecuciones de flujo han fallado en {exec_messages.get(exec_type, 'el período especificado:')}."

    if exec_type == TipoEjecucion.DIARIA:
        date_format = '%d/%m/%Y'
        subject = f"Prefect: Informe Diario de Flujos Fallidos - Día {fecha_inicial.strftime(date_format)}"
    elif exec_type == TipoEjecucion.SEMANAL:
        date_format = '%d/%m/%Y'
        subject = f"Prefect: Informe Semanal de Flujos Fallidos - Semana {fecha_inicial.strftime(date_format)}"
    elif exec_type == TipoEjecucion.MENSUAL:
        date_format = '%m/%Y'
        subject = f"Prefect: Informe Mensual de Flujos Fallidos - Mes {fecha_inicial.strftime(date_format)}"
    elif exec_type == TipoEjecucion.PERSONALIZADA:
        date_format = '%d/%m/%Y %H:%M:%S'
        subject = f"Prefect: Informe de Flujos Fallidos - {fecha_inicial.strftime(date_format)} al {fecha_final.strftime(date_format)}"
    else:
        raise ValueError(f"Tipo de ejecucion '{exec_type.value}' no reconocido.")

    if failed_flow_runs_df.empty:
        msg = f"No hubo ejecuciones de flujo fallidas en {exec_messages.get(exec_type, 'el período especificado')}."
        is_html = False
    else:
        # Generar URL de UI de Prefect
        start_date_str = fecha_inicial.astimezone(pytz.utc).replace(tzinfo=None).isoformat(timespec='milliseconds') + 'Z'
        end_date_str = fecha_final.astimezone(pytz.utc).replace(tzinfo=None).isoformat(timespec='milliseconds') + 'Z'
        ui_url = get_prefect_url()
        flow_runs_ui_url = f"{ui_url}flow-runs?type=range&hide-subflows=true&endDate={end_date_str}&startDate={start_date_str}&state=Failed"

        # Generar la tabla de resumen y agregar los subflujos a los flujos padres
        summary_table_html = generate_summary_table(failed_flow_runs_df.copy())
        formatted_failed_flow_runs = add_subflows_to_parent_flows(failed_flow_runs_df.copy())

        # Extraer el HTML y el CSS de la tabla de resumen para ponerlo en el header del mail
        soup = BeautifulSoup(summary_table_html, 'html.parser')
        style_tag = soup.find('style')
        style_str = str(style_tag) if style_tag else ''
        table_tag = soup.find('table')
        table_str = str(table_tag) if table_tag else ''

        # Leer el template de email
        template_path = os.path.join(os.path.dirname(__file__), 'email_template.html')
        with open(template_path, 'r', encoding='utf-8') as file:
            template_content = file.read()

        # Renderizar el email
        email_template = Template(template_content)
        msg = email_template.render(
            style_str=style_str,
            header_message=header_message,
            table_str=table_str,
            failed_flow_runs=formatted_failed_flow_runs,
            flow_runs_ui_url=flow_runs_ui_url,
        )

        is_html = True

    try:
        send_email(
            mail_to=destinatarios,
            subject=subject,
            body=msg,
            is_html=is_html
        )
    except Exception as e:
        error_msg = f"Error enviando email: {e}"
        return error_msg

    return "Email enviado."


if __name__ == '__main__':
    # pass
    data = {
        'flow_id': ['flow1', 'flow2', 'flow3', 'flow4', 'flow5', 'flow6'],
        'flow_name': ['carga-accesos-powerbi', 'conectar-power-bi', 'parent-flow', 'subflow', 'flow_september', 'flow_august'],
        'deployment_id': ['deploy1', 'deploy2', 'deploy3', 'deploy4', 'deploy5', 'deploy6'],
        'deployment_name': ['deployment1', 'deployment2', 'deployment3', 'deployment4', 'deployment5', 'deployment6'],
        'deployment_entrypoint': ['entry1', 'entry2', 'entry3', 'entry4', 'entry5', 'entry6'],
        'id': ['run1', 'run2', 'run3', 'run4', 'run5', 'run6'],
        'flow_run_name': ['run_name1', 'run_name2', 'parent_run', 'subflow_run', 'run_september', 'run_august'],
        'state': [{'message': 'Failed due to timeout'}, {'message': 'Failed due to error'}, 
            {'message': 'Failed due to timeout'}, {'message': 'Failed due to timeout'}, 
            {'message': 'Failed due to timeout'}, {'message': 'Failed due to timeout'}],
        'start_time': [datetime.datetime(2024, 8, 1, 10, 0), datetime.datetime(2024, 8, 2, 11, 0), datetime.datetime(2024, 10, 3, 12, 0),
            datetime.datetime(2024, 10, 3, 13, 0),
            datetime.datetime(2024, 9, 1, 10, 0),
            datetime.datetime(2024, 8, 2, 11, 0)],
        'end_time': [datetime.datetime(2024, 8, 1, 11, 0), datetime.datetime(2024, 8, 2, 12, 0), datetime.datetime(2024, 10, 3, 13, 0),
            datetime.datetime(2024, 10, 3, 14, 0),
            datetime.datetime(2024, 9, 1, 11, 0),
            datetime.datetime(2024, 8, 2, 12, 0)],
        'ui_url': ['http://example.com/run1', 'http://example.com/run2', 
            'http://example.com/run3', 'http://example.com/run4', 
            'http://example.com/run5', 'http://example.com/run6'],
        'parent_flow_run_id': [None, None, None, 'run3', None, None]  # Indicating that 'run4' is a subflow of 'run3'
    }

    _failed_flow_runs = pd.DataFrame(data)

    _destinatarios = "lucas.depetris@consulters.com.ar"
    _fecha_ejecucion = datetime.datetime.now() - datetime.timedelta(days=30)
    _fecha_final = datetime.datetime.now()
    _exec_type = TipoEjecucion.MENSUAL

    send_report_failed_flows(_failed_flow_runs, _destinatarios, _fecha_ejecucion, _fecha_final, _exec_type)

    # send_email(
    #         mail_to=destinatarios,
    #         subject=subject,
    #         body=msg
    #     )
