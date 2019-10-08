# Copyright (C) 2016-2019  Sogo Mineo
# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
#
# This file is part of the project DC2-PostgreSQL
# DC2-PostgreSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .. import algobase
from .. import sourcetable
from .. import common


class Algo_base_PsfFlux(algobase.Algo):
    fluxes = [
        {
            "flux": 'base_PsfFlux_flux',
        },
    ]
    fluxerrs = [
        {
            "flux": 'base_PsfFlux_flux',
            "fluxerr": 'base_PsfFlux_fluxSigma',
        },
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        #self.sourceTable = sourceTable.cutout_subtable("base_PsfFlux_")

        temp_sourceTable = sourceTable.cutout_subtable("base_PsfFlux_")
        fields = temp_sourceTable.fields
        flux = fields["base_PsfFlux_instFlux"].data
        fluxerr = fields["base_PsfFlux_instFluxErr"].data
        mag = common.flux_to_mag(flux)
        magerr = common.flux_to_magerr(flux, fluxerr)
        fields["mag"] = sourcetable.Field("mag", "Scalar", "", mag,
                                          "Computed magnitude", None)
        fields["magerr"] = sourcetable.Field("magerr", "Scalar", "", magerr,
                                          "Computed magnitude error", None)
        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots,
                                                   sourceTable.fitsheader)
