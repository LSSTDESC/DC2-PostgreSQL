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

import itertools


class Algo_modelfit_CModel(algobase.Algo):
    fluxes = [
        {
            "flux": 'modelfit_CModel{}_flux{}'.format(infix, suffix)
        }
        for infix, suffix in itertools.product(
            ["_initial", "_exp", "_dev", ""],
            ["", "_inner"],
        )
    ]

    fluxerrs = [
        {
            "flux": 'modelfit_CModel{}_flux'.format(infix),
            "fluxerr": 'modelfit_CModel{}_fluxSigma'.format(infix)
        }
        for infix in ["_initial", "_exp", "_dev", ""]
    ]

    renamerules = [
        (r'modelfit_', ''),
    ]

    def __init__(self, sourceTable):
        #self.sourceTable = sourceTable.cutout_subtable("modelfit_CModel_")
        temp_sourceTable = sourceTable.cutout_subtable("modelfit_CModel_")
        fields = temp_sourceTable.fields
        flux = fields["modelfit_CModel_instFlux"].data
        fluxerr = fields["modelfit_CModel_instFluxErr"].data
        mag = common.flux_to_mag(flux)
        magerr = common.flux_to_magerr(flux,fluxerr)
        fields["mag_cmodel"] = sourcetable.Field("mag_cmodel", "Scalar", "",
                                                 mag, "CModel magnitude", None)
        fields["magerr_cmodel"] = sourcetable.Field("magerr_cmodel", "Scalar",
                                                    "", magerr, 
                                                    "CModel mag error", None)
        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots,
                                                   sourceTable.fitsheader)
        # Cut out stuff unused by dpdd
        junk = self.sourceTable.cutout_subtable("modelfit_CModel_initial_")
        junk = self.sourceTable.cutout_subtable("modelfit_CModel_exp_")
        junk = self.sourceTable.cutout_subtable("modelfit_CModel_dev_")
