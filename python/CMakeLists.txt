include(init_python)
init_python(2.7)

#-----------------------------------------------------------------
# Find an existing ert.ecl disribution

find_python_package(ecl 2.1.0 ${PYTHON_INSTALL_PREFIX})
find_python_package(res 2.1.0 ${PYTHON_INSTALL_PREFIX})
find_python_package(cwrap 0.0.0 ${PYTHON_INSTALL_PREFIX})
find_python_package(numpy 1.7.1 ${PYTHON_INSTALL_PREFIX})

# libres python needs ecl, cwrap and numpy
if (NOT DEFINED PY_ecl OR NOT DEFINED PY_cwrap OR NOT DEFINED PY_numpy OR NOT DEFINED PY_res)
    message(SEND_ERROR "res, ecl, cwrap, or numpy module not found ")
endif()

#-----------------------------------------------------------------

find_python_package(matplotlib 1.2.0  ${PYTHON_INSTALL_PREFIX})
find_python_package(pandas     0.15.1 ${PYTHON_INSTALL_PREFIX})
find_python_package(scipy      0.12.1 ${PYTHON_INSTALL_PREFIX})

if(NOT DEFINED PY_matplotlib OR NOT DEFINED PY_scipy OR NOT DEFINED PY_pandas)
  message(SEND_ERROR "Packages matplotlib, scipy and pandas are required")
endif()

if (GUI) 
    if (PYTHON_VERSION_MAJOR EQUAL 3)
        find_python_package(PyQt5  5.7.1  ${PYTHON_INSTALL_PREFIX})
        if (NOT DEFINED PY_PyQt5)
            message(SEND_ERROR "PyQt5 module not found - can not build GUI")
        endif()
    else()
        find_python_package(PyQt4  4.8.0  ${PYTHON_INSTALL_PREFIX})
        if (NOT DEFINED PY_PyQt4)
            message(SEND_ERROR "PyQt4 module not found - can not build GUI")
        endif()
    endif()
endif()

if (BUILD_TESTS)
   add_subdirectory( tests )
endif()
add_subdirectory( python )
