from pathlib import Path

from justices.base import BaseJustice, parse_args


class TribunalAdministratif(BaseJustice):
    filename_xml_col = "Nom du fichier .xml"
    filename_zip_col = "Nom du fichier .zip"
    date_col = "Date de lecture"
    on_bad_lines = "skip"

    def _build_xml_path(self, name: str) -> str:
        matches = list(Path(self.folder_download).rglob(name))
        if matches:
            return str(matches[0])
        return f"{self.folder_download}/{name}"


if __name__ == "__main__":
    args = parse_args()

    obj = TribunalAdministratif(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DTA/TA_documents_reverses.csv"
        },
        folder_download="data/unprocessed/tribunal_administratif",
        base_url="https://opendata.justice-administrative.fr/DTA/YEAR/MONTH/TA_YEARMONTH.zip",
    )
    obj.run(repo_id=f"{args.user_id}/tribunal-administratif-full-documents")
