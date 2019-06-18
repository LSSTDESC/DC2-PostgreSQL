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

class Algo_base_PixelFlags(algobase.Algo):
    renamerules = [
        (r'base_PixelFlags_flag', 'PixelFlags'),
    ]

    def __init__(self, sourceTable):
        temp_sourceTable = sourceTable.cutout_subtable("base_PixelFlags_")

        # create 'good' flag out of a collection of pixel flags
        fields = temp_sourceTable.fields
        notgood = fields["base_PixelFlags_flag_edge"].data
        for sfx in ["interpolatedCenter","saturatedCenter","crCenter",
                    "bad", "suspectCenter", "clipped"]:
            notgood = notgood.__or__(fields["base_PixelFlags_flag_" + sfx].data)
        good = notgood.__eq__(0)
        fields["good"] = sourcetable.Field("good", "Scalar", "", good,
                                           "Good pixel flags", None)

        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots,
                                                   sourceTable.fitsheader)

        
        
        
