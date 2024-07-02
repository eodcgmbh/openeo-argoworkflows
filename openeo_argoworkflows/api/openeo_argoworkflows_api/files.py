import fsspec
import re

from fastapi import Depends, HTTPException, Request
from fastapi.responses import StreamingResponse, Response
from pathlib import Path
from pydantic import validator
from pydantic.dataclasses import dataclass
from os.path import splitext

from typing import Optional
from openeo_fastapi.client.files import FilesRegister
from openeo_fastapi.client.auth import User

from openeo_argoworkflows_api.auth import ExtendedAuthenticator

fs = fsspec.filesystem(protocol="file")

@dataclass
class ByteRange:
    start: int
    end: Optional[int]
    range: int = None

    @validator("start", pre=True, always=True)
    def set_start(v):
        if v is None:
            return 0
        return v

    @validator("range", pre=False, always=True)
    def set_range(v, values):
        if values["end"] is not None:
            if not values["start"] < values["end"]:
                raise ValueError(
                    f"{values['end']} must be greater than {values['start']}"
                )
            return values["end"] - values["start"] + 1



class ArgoFileRegister(FilesRegister):
    def __init__(self, settings, links) -> None:
        super().__init__(settings, links)


    def compile_byte_ranges(self, byte_range: str) -> list[ByteRange]:
        """Take the byte range string and return list of ranges."""
        if not re.search(r"bytes=", byte_range):
            raise ValueError("Only byte ranges are currently supported, i.e. bytes=")

        r = r"\w+-\w+|\w+-|-\w+"
        bytes_eval = lambda a: int(a) if a != "" else None

        ranges = [
            ByteRange(start=bytes_eval(x.split("-")[0]), end=bytes_eval(x.split("-")[1]))
            for x in re.findall(r, byte_range)
        ]

        return ranges


    def validate_path(self, path: str, user: User):
        """Does the give path exist in the user workspace?
        If yes, return the absolute path."""

        # Get the user workspace
        user_workspace = self.settings.OPENEO_WORKSPACE_ROOT / str(user.user_id)
        # Does the file exist in the workspace?
        absolute_path = user_workspace / path
        try:
            fs.exists(absolute_path)
            if not fs.isfile(absolute_path):
                raise HTTPException(
                    status_code=405,
                    detail=f"Path must lead to file, {absolute_path} resolves to a directory.",
                )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="File not found in user workspace.")

        return absolute_path


    def file_header(
        self,
        path: str,
        user: User = Depends(ExtendedAuthenticator.signed_url_or_validate)
    ):
        """Get the headers for a file."""

        absolute_path = self.validate_path(path, user)
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Type": "application/octet-stream",
                "Content-Length": str(fs.size(absolute_path)),
            },
        )


    def download_file(
        self,
        path: str,
        request: Request,
        user: User = Depends(ExtendedAuthenticator.signed_url_or_validate)
    ):
        """
        Download a file from the workspace.
        """

        def iterfile(path: Path, range: ByteRange = None):
            # 1024 * 1024 is roughly 1Mb * this by the number of Mb we want to try and serve
            chunk_size = ( 1024 * 1024 ) * 20
            
            with open(path, mode="rb") as file_like:
                if range:
                    file_like.seek(range.start)
                    if range.end:
                        yield file_like.read(range.range)
                    else:
                        while chunk := file_like.read(chunk_size):
                            yield chunk
                else:
                    while chunk := file_like.read(chunk_size):
                        yield chunk

        absolute_path = self.validate_path(path, user)

        # Set media type for the file
        extention = splitext(absolute_path)[1]
        mime_types = {
            ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
            ".nc": "application/netcdf",
            ".json": "application/json"
        }

        try:
            mime_type = mime_types[extention]
        except ValueError as e:
            raise HTTPException(status_code=400, detail=e.args[0])

        range_request = request.headers.get("Range")
        fsize = fs.size(absolute_path)

        if range_request:
            ranges = self.compile_byte_ranges(range_request)
            if len(ranges) != 1:
                raise HTTPException(
                    status_code=416, detail="multipart range request is not supported!"
                )
            else:
                range = ranges[0]
                if range.range and range.range > fsize:
                    # OpenEO Editor doesn't use pre-flight HEAD request, so sometimes a file might be smaller than the default
                    # 65536 byte request that is set by the editor.
                    range.end = fsize
                    range.range = range.end - range.start + 1

                if range.end is None:
                    range.end = fsize

                return StreamingResponse(
                    status_code=206,
                    content=iterfile(absolute_path, range),
                    media_type=mime_type,
                    headers={
                        "Content-Range": "{}-{}/{}".format(range.start, range.end, fsize),
                    },
                )
        else:
            return StreamingResponse(
                status_code=200, content=iterfile(absolute_path), media_type=mime_type
            )
