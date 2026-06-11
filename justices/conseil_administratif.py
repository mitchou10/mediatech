from datetime import datetime

from justices.base import BaseJustice, parse_args


class ConseilDadministratif(BaseJustice):
    filename_xml_col = "Nom_du_fichier__xml"
    filename_zip_col = "Nom_du_fichier__zip"
    date_col = "Date_de_lecture"


if __name__ == "__main__":
    args = parse_args()
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    print(79 * "*")
    print(f"Start: {start_date} end: {end_date}")

    obj = ConseilDadministratif(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DCA/CAA_documents_reverses.csv"
        },
        folder_download="data/unprocessed/administrative_courts",
        base_url="https://opendata.justice-administrative.fr/DCA/YEAR/MONTH/CAA_YEARMONTH.zip",
    )
    obj.run(
        start_date=start_date,
        end_date=end_date,
        repo_id=f"{args.user_id}/conseil-administratives-appel-full-documents",
    )
