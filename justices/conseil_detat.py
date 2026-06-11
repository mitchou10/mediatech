from datetime import datetime

from justices.base import BaseJustice, parse_args


class ConseilDetat(BaseJustice):
    filename_xml_col = "Nom du fichier .xml"
    filename_zip_col = "Nom du fichier .zip"
    date_col = "Date de lecture"


if __name__ == "__main__":
    args = parse_args()
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    print(79 * "*")
    print(f"Start: {start_date} end: {end_date}")

    obj = ConseilDetat(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DCE/CE_documents_reverses.csv"
        },
        folder_download="data/unprocessed/jurisprudence",
        base_url="https://opendata.justice-administrative.fr/DCE/YEAR/MONTH/CE_YEARMONTH.zip",
    )
    obj.run(
        start_date=start_date,
        end_date=end_date,
        repo_id=f"{args.user_id}/conseil-detat-full-documents",
    )
