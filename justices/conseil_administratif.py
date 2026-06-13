from justices.base import BaseJustice, parse_args


class ConseilDadministratif(BaseJustice):
    filename_xml_col = "Nom_du_fichier__xml"
    filename_zip_col = "Nom_du_fichier__zip"
    date_col = "Date_de_lecture"


if __name__ == "__main__":
    args = parse_args()

    obj = ConseilDadministratif(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DCA/CAA_documents_reverses.csv"
        },
        folder_download="data/unprocessed/administrative_courts",
        base_url="https://opendata.justice-administrative.fr/DCA/YEAR/MONTH/CAA_YEARMONTH.zip",
    )
    obj.run(repo_id=f"{args.user_id}/conseil-administratives-appel-full-documents")
