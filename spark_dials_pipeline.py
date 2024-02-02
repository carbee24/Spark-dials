from dials.command_line.dials_import import *
from dials.command_line.find_spots import *
# from options import *
from dials.util.options import *
from dxtbx.model import ExperimentList
import sys
import dials.util.masking
from dxtbx.imageset import ImageSequence
import functools
from time import time
import os
import copy
import threading

from pyspark import SparkConf, SparkContext, AccumulatorParam


start_time = time()
# os.environ['PYSPARK_PYTHON'] = '/home/Data/local/opt/xtal/ccp4-8.0/libexec/python3.7'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/Data/local/opt/xtal/ccp4-8.0/libexec/python3.7'
# conf = SparkConf().setMaster("yarn").setAppName("pipeline_test").set("spark.executor.memory", "40g")\
#        .set("spark.driver.memory", "150g").set('spark.defalut.parallelism', '80')
os.environ['PYSPARK_PYTHON'] = '/home/Data/local/opt/xtal/ccp4-8.0/libexec/python3.7'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/Data/local/opt/xtal/ccp4-8.0/libexec/python3.7'
os.environ['HADOOP_CONF_DIR'] = '/data/zd/local/hadoop/hadoop-3.3.4/etc/hadoop'
os.environ['YARN_CONF_DIR'] = '/data/zd/local/hadoop/hadoop-3.3.4/etc/hadoop'
nproc = 90
conf = SparkConf().setMaster("yarn").setAppName("Tau-nat")\
               .set("spark.driver.memory", "20g") \
                   .set("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"]) \
                    .set('spark.executor.memory', '4g').set('spark.executor.cores', 4).set("spark.driver.host", "hadoop-node1")
                    # .set("spark.num.executors", "25")
#             "local[{}]".format(nproc)   "yarn"
sc = SparkContext(conf=conf)



#########################################     PIPELINE DEFINITION      ########################################

def generate_input_scope(read_experiments=False, read_experiments_from_images=False, read_reflections=False):
        """
        Generate the required input scope.

        :return: The input phil scope
        """
        from dials.util.phil import parse

        # Create the input scope
        require_input_scope = (
            read_experiments
            or read_reflections
            or read_experiments_from_images
        )
        if not require_input_scope:
            return None
        input_phil_scope = parse("input {}")
        main_scope = input_phil_scope.get_without_substitution("input")
        assert len(main_scope) == 1
        main_scope = main_scope[0]

        # Add the experiments phil scope
        if read_experiments or read_experiments_from_images:
            phil_scope = parse(
                f"""
        experiments = None
          .type = experiment_list(check_format={True!r})
          .multiple = True
          .help = "The experiment list file path"
      """
            )
            main_scope.adopt_scope(phil_scope)

        # If reading images, add some more parameters
        if read_experiments_from_images:
            main_scope.adopt_scope(tolerance_phil_scope)

        # Add the reflections scope
        if read_reflections:
            phil_scope = parse(
                """
        reflections = None
          .type = reflection_table
          .multiple = True
          .help = "The reflection table file path"
      """
            )
            main_scope.adopt_scope(phil_scope)

        # Return the input scope
        return input_phil_scope


###############################################################################################################

###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                    DIALS  IMPORT                      ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

start_import_time = time()
args = None
logger = logging.getLogger("dials.spark_dials_pipeline")
importer = ImageImporter()

"""Parse the options."""

# Parse the command line arguments in two passes to set up logging early
params, options = importer.parser.parse_args( args=args, show_diff_phil=False, quick_parse=True)

# print(dir(params.input))

'''
print(dir(params))
print(options)
['__call__', '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__inject__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__phil_call__', '__phil_get__', '__phil_join__', '__phil_name__', '__phil_parent__', '__phil_path__', '__phil_path_and_value__', '__phil_set__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 'format', 'geometry', 'identifier_type', 'input', 'lookup', 'output']
Namespace(attributes_level=0, expert_level=0, export_autocomplete_hints=False, help=0, phil=None, show_config=False, sort=False, verbose=0)
'''
# Configure logging, if this is the main process
# if __name__ == "__main__":
from dials.util import log, tabulate

log.config(verbosity=options.verbose, logfile="spark_dials_pipeline.log")

from dials.util.version import dials_version

logger.info(dials_version())

# Parse the command line arguments completely
if params.input.ignore_unhandled:
    params, options, unhandled = importer.parser.parse_args(
        args=args, show_diff_phil=False, return_unhandled=True
    )
    # Remove any False values from unhandled (eliminate empty strings)
    unhandled = [x for x in unhandled if x]
else:
    params, options = importer.parser.parse_args(args=args, show_diff_phil=False)
    unhandled = None

# print(len(params.input.experiments))  1
# print(params.input.experiments[0].data)  ExperimentList([<dxtbx_model_ext.Experiment object at 0x7fde81e36620>])
# print(len(params.input.experiments[0].data.imagesets().count))
'''
print(dir(params))
print(options)
print(unhandled)
['__call__', '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__inject__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__phil_call__', '__phil_get__', '__phil_join__', '__phil_name__', '__phil_parent__', '__phil_path__', '__phil_path_and_value__', '__phil_set__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 'format', 'geometry', 'identifier_type', 'input', 'lookup', 'output']
Namespace(attributes_level=0, expert_level=0, export_autocomplete_hints=False, help=0, phil=None, show_config=False, sort=False, verbose=0)
[]
'''

# Log the diff phil
diff_phil = importer.parser.diff_phil.as_str()
# print(diff_phil)
if diff_phil:
    logger.info("The following parameters have been modified:\n")
    logger.info(diff_phil)


 # Print a warning if something unhandled
if unhandled:
    msg = "Unable to handle the following arguments:\n"
    msg += "\n".join(["  %s" % a for a in unhandled])
    msg += "\n"
    logger.warning(msg)

# Print help if no input
if len(params.input.experiments) == 0 and not (
    params.input.template or params.input.directory
):
    importer.parser.print_help()
    sys.exit(0)

# Re-extract the imagesets to rebuild experiments from
# Get the experiments
experiments = flatten_experiments(params.input.experiments)
# print(experiments) ExperimentList([<dxtbx_model_ext.Experiment object at 0x7f8018058760>])
# print(len(experiments)) 1

# Check we have some filenames
if len(experiments) == 0:
    # FIXME Should probably make this smarter since it requires editing here
    # and in dials.import phil scope
    try:
        format_kwargs = {
            "dynamic_shadowing": params.format.dynamic_shadowing,
            "multi_panel": params.format.multi_panel,
        }
    except AttributeError:
        format_kwargs = None

    # Check if a template has been set and print help if not, otherwise try to
    # import the images based on the template input
    if len(params.input.template) > 0:
        experiments = ExperimentListFactory.from_templates(
            params.input.template,
            image_range=params.geometry.scan.image_range,
            format_kwargs=format_kwargs,
        )
        if len(experiments) == 0:
            raise Sorry(
                "No experiments found matching template %s"
                % params.input.experiments
            )
    elif len(params.input.directory) > 0:
        experiments = ExperimentListFactory.from_filenames(
            params.input.directory, format_kwargs=format_kwargs
        )
        if len(experiments) == 0:
            raise Sorry(
                "No experiments found in directories %s" % params.input.directory
            )
    else:
        raise Sorry("No experiments found")

# TODO (Nick):  This looks redundant as the experiments are immediately discarded.
#               verify this, and remove if it is.
if params.identifier_type:
    generate_experiment_identifiers(experiments, params.identifier_type)
# Get a list of all imagesets
imagesets = experiments.imagesets()
# print(len(imageset_list))
# # Return the experiments
# return imageset_list

metadata_updater = MetaDataUpdater(params)
experiments = metadata_updater(imagesets)
# Compute some numbers
num_sweeps = 0
num_still_sequences = 0
num_stills = 0
num_images = 0
# importing a lot of experiments all pointing at one imageset should
# work gracefully
counted_imagesets = []
for e in experiments:
    if e.imageset in counted_imagesets:
        continue
    if isinstance(e.imageset, ImageSequence):
        if e.imageset.get_scan().is_still():
            num_still_sequences += 1
        else:
            num_sweeps += 1
    else:
        num_stills += 1
    num_images += len(e.imageset)
    counted_imagesets.append(e.imageset)
format_list = {str(e.imageset.get_format_class()) for e in experiments}
# Print out some bulk info
logger.info("-" * 80)
for f in format_list:
    logger.info("  format: %s", f)
logger.info("  num images: %d", num_images)
logger.info("  sequences:")
logger.info("    still:    %d", num_still_sequences)
logger.info("    sweep:    %d", num_sweeps)
logger.info("  num stills: %d", num_stills)
# Print out info for all experiments
for experiment in experiments:
    # Print some experiment info - override the output of image range
    # if appropriate
    image_range = params.geometry.scan.image_range
    if isinstance(experiment.imageset, ImageSequence):
        imageset_type = "sequence"
    else:
        imageset_type = "stills"
    logger.debug("-" * 80)
    logger.debug("  format: %s", str(experiment.imageset.get_format_class()))
    logger.debug("  imageset type: %s", imageset_type)
    if image_range is None:
        logger.debug("  num images:    %d", len(experiment.imageset))
    else:
        logger.debug("  num images:    %d", image_range[1] - image_range[0] + 1)
    logger.debug("")
    logger.debug(experiment.imageset.get_beam())
    logger.debug(experiment.imageset.get_goniometer())
    logger.debug(experiment.imageset.get_detector())
    logger.debug(experiment.imageset.get_scan())
# Only allow a single sequence
if params.input.allow_multiple_sequences is False:
    importer.assert_single_sequence(experiments, params)
# Write the experiments to file
# def threadImportRun(experiments, params):
#     importer.write_experiments(experiments, params)
# t = threading.Thread(target=threadImportRun, args=())
importer.write_experiments(experiments, params)
end_import_time = time()
logger.info('Import cost %s s'%(end_import_time-start_import_time))

###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                    DIALS  FIND_SPOTS                  ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

#########################################     FIND_SPOTS DEFINITION    ########################################

from dials.algorithms.spot_finding.finder import *
import os
# import time
# from dials.model.data.boost_python import runPixelListLabeller
from dials.model.data import PixelList, PixelListLabeller





class runExtractPixelsFromImage(ExtractPixelsFromImage):
    """
    A class to extract pixels from a single image
    """
    def __init__(
        self,
        imageset,
        threshold_function,
        mask,
        region_of_interest,
        max_strong_pixel_fraction,
        compute_mean_background,
    ):
        """
        Initialise the class

        :param imageset: The imageset to extract from
        :param threshold_function: The function to threshold with
        :param mask: The image mask
        :param region_of_interest: A region of interest to process
        :param max_strong_pixel_fraction: The maximum fraction of pixels allowed
        """
        super(runExtractPixelsFromImage, self).__init__(
            imageset,
            threshold_function,
            mask,
            region_of_interest,
            max_strong_pixel_fraction,
            compute_mean_background,
        )
    
    def __call__(self, index):
        """
        Extract strong pixels from an image

        :param index: The index of the image
        """
        # print("start %s"%index)
        # Get the frame number
        if isinstance(self.imageset, ImageSequence):
            frame = self.imageset.get_array_range()[0] + index
        else:
            ind = self.imageset.indices()
            if len(ind) > 1:
                assert all(i1 + 1 == i2 for i1, i2 in zip(ind[0:-1], ind[1:-1]))
            frame = ind[index]
        
        # Create the list of pixel lists
        pixel_list = []

        # Get the image and mask
        image = self.imageset.get_corrected_data(index)
        mask = self.imageset.get_mask(index)
        # print((image[0][0:1, 0:1]))
        # print(mask)

        # Set the mask
        if self.mask is not None:
            assert len(self.mask) == len(mask)
            mask = tuple(m1 & m2 for m1, m2 in zip(mask, self.mask))

        logger.debug(
            "Number of masked pixels for image %i: %i",
            index,
            sum(m.count(False) for m in mask),
        )

        # Add the images to the pixel lists
        num_strong = 0
        average_background = 0
        # print(self.region_of_interest)
        for im, mk in zip(image, mask):
            if self.region_of_interest is not None:
                x0, x1, y0, y1 = self.region_of_interest
                height, width = im.all()
                assert x0 < x1, "x0 < x1"
                assert y0 < y1, "y0 < y1"
                assert x0 >= 0, "x0 >= 0"
                assert y0 >= 0, "y0 >= 0"
                assert x1 <= width, "x1 <= width"
                assert y1 <= height, "y1 <= height"
                im_roi = im[y0:y1, x0:x1]
                mk_roi = mk[y0:y1, x0:x1]
                tm_roi = self.threshold_function.compute_threshold(im_roi, mk_roi)
                threshold_mask = flex.bool(im.accessor(), False)
                threshold_mask[y0:y1, x0:x1] = tm_roi
            else:
                threshold_mask = self.threshold_function.compute_threshold(im, mk)

            # Add the pixel list
            plist = PixelList(frame, im, threshold_mask)
            # print(plist)
            pixel_list.append(plist)

            # Get average background
            if self.compute_mean_background:
                background = im.as_1d().select((mk & ~threshold_mask).as_1d())
                average_background += flex.mean(background)

            # Add to the spot count
            num_strong += len(plist)
        # Make average background
        average_background /= len(image)

        # Check total number of strong pixels
        if self.max_strong_pixel_fraction < 1:
            num_image = 0
            for im in image:
                num_image += len(im)
            max_strong = int(math.ceil(self.max_strong_pixel_fraction * num_image))
            if num_strong > max_strong:
                raise RuntimeError(
                    f"""
          The number of strong pixels found ({num_strong}) is greater than the
          maximum allowed ({max_strong}). Try changing spot finding parameters
        """
                )

        # Print some info
        if self.compute_mean_background:
            logger.info(
                "Found %d strong pixels on image %d with average background %f",
                num_strong,
                frame + 1,
                average_background,
            )
        else:
            logger.info("Found %d strong pixels on image %d", num_strong, frame + 1)
        # print("end %s"%index)

        # Return the result
        return pixel_list


class runExtractSpots(ExtractSpots):
    def __init__(
        self,
        threshold_function=None,
        mask=None,
        region_of_interest=None,
        max_strong_pixel_fraction=0.1,
        compute_mean_background=False,
        mp_method=None,
        mp_nproc=1,
        mp_njobs=1,
        mp_chunksize=1,
        min_spot_size=1,
        max_spot_size=20,
        filter_spots=None,
        no_shoeboxes_2d=False,
        min_chunksize=50,
        write_hot_pixel_mask=False,
    ):
        super(runExtractSpots, self).__init__(
            threshold_function,
            mask,
            region_of_interest,
            max_strong_pixel_fraction,
            compute_mean_background,
            mp_method,
            mp_nproc,
            mp_njobs,
            mp_chunksize,
            min_spot_size,
            max_spot_size,
            filter_spots,
            no_shoeboxes_2d,
            min_chunksize,
            write_hot_pixel_mask,
        )
    def __call__(self, imageset):
        """
        Find the spots in the imageset

        :param imageset: The imageset to process
        :return: The list of spot shoeboxes
        """
        if not self.no_shoeboxes_2d:
            return self._find_spots(imageset)
        else:
            return self._find_spots_2d_no_shoeboxes(imageset)

    def _find_spots(self, imageset):
        """
        Find the spots in the imageset

        :param imageset: The imageset to process
        :return: The list of spot shoeboxes
        """
        # Change the number of processors if necessary
        mp_nproc = self.mp_nproc
        mp_njobs = self.mp_njobs
        if mp_nproc is libtbx.Auto:
            mp_nproc = available_cores()
            logger.info(f"Setting nproc={mp_nproc}")
        if mp_nproc * mp_njobs > len(imageset):
            mp_nproc = min(mp_nproc, len(imageset))
            mp_njobs = int(math.ceil(len(imageset) / mp_nproc))

        mp_method = self.mp_method
        mp_chunksize = self.mp_chunksize

        if mp_chunksize is libtbx.Auto:
            mp_chunksize = self._compute_chunksize(
                len(imageset), mp_njobs * mp_nproc, self.min_chunksize
            )
            logger.info("Setting chunksize=%i", mp_chunksize)
        
        len_by_nproc = int(math.floor(len(imageset) / (mp_njobs * mp_nproc)))
        if mp_chunksize > len_by_nproc:
            mp_chunksize = len_by_nproc
        if mp_chunksize == 0:
            mp_chunksize = 1
        assert mp_nproc > 0, "Invalid number of processors"
        assert mp_njobs > 0, "Invalid number of jobs"
        assert mp_njobs == 1 or mp_method is not None, "Invalid cluster method"
        assert mp_chunksize > 0, "Invalid chunk size"

        # The extract pixels function
        function = runExtractPixelsFromImage(
            imageset=imageset,
            threshold_function=self.threshold_function,
            mask=self.mask,
            max_strong_pixel_fraction=self.max_strong_pixel_fraction,
            compute_mean_background=self.compute_mean_background,
            region_of_interest=self.region_of_interest,
        )

        # The indices to iterate over
        indices = list(range(len(imageset)))

        # Initialise the pixel labeller
        num_panels = len(imageset.get_detector())
        pixel_labeller = [PixelListLabeller() for p in range(num_panels)]
       
        class PixelListLabellerParam(AccumulatorParam):
            def zero(self, value):
                return value
            
            def addInPlace(self, val1, val2):
                
                plist = val2.get_pixel_list()
                val1.add(plist)
                print(len(val1.coords()))
                return val1
        # pixel_labeller = runPixelListLabeller.PixelListLabeller()
        # print(pixel_labeller.num_frames())
        # single_pixel_labeller = runPixelListLabeller.PixelListLabeller()
        # start = time.time()
        
      
        # pixel_labeller_accumulator = sc.accumulator(pixel_labeller[0], PixelListLabellerParam()) 
        # single_pixel_labeller_accumulator = sc.accumulator(single_pixel_labeller, PixelListLabellerParam())
        global nproc
        processRDD = sc.parallelize(indices, nproc)
        
        print("默认分区数:", processRDD.getNumPartitions())
        # print(indices)

        

        # Do the processing
        logger.info("Extracting strong pixels from images")

        # def runTask(task):
        #     result = function(task)
        #     assert len(pixel_labeller) == len(result), "Inconsistent size"
        #     for plabeller, plist in zip(pixel_labeller, result):
        #         plabeller.add(plist)
        #     result.clear()
        # processRDD.foreach(runTask)
        
        
        def runTask(task):
            # single_pixel_labeller = runPixelListLabeller.PixelListLabeller()
            result = function(task)
            plist = result[0]
            # single_pixel_labeller.set_pixel_list(plist)
            # pixel_labeller_accumulator.add(single_pixel_labeller)
            # print(len(pixel_labeller_accumulator.coords()))
            result.clear()
            return (task, plist)
        
        after_find_strong_spots_collection = processRDD.map(runTask).collect()
        sorted(after_find_strong_spots_collection, key = lambda tup: tup[0])
        # processRDD.foreach(runTask)
        # print(len(pixel_labeller_accumulator.value.coords()))
        # labeller = after_find_strong_spots_colletction[0][1]
        # print(len(labeller.labels_3d))
        # print(len(after_find_strong_spots_colletction[0][1].coords()))

        # for plist in after_find_strong_spotsRDD:
        #     print(plist)
        
        # for task in indices:
        #     result = function(task)
        #     assert len(pixel_labeller) == len(result), "Inconsistent size"
        #     for plabeller, plist in zip(pixel_labeller, result):
        #         plabeller.add(plist)
        #     result.clear()
        # print(len(result_list))
        # print(len(result_list_broadcast.value))
        
        
        for i, plist in after_find_strong_spots_collection:
            pixel_labeller[0].add(plist)
        # print(pixel_labeller_accumulator.value[0].num_frames())
        # print(len(pixel_labeller_accumulator.value.coords()))
        # print(pixel_labeller.num_frames(),len(pixel_labeller.values()))
        # pixel_labeller = [after_find_strong_spots_colletction[0][1]]
        # Create shoeboxes from pixel list
        return pixel_list_to_reflection_table(
            imageset,
            pixel_labeller,
            filter_spots=self.filter_spots,
            min_spot_size=self.min_spot_size,
            max_spot_size=self.max_spot_size,
            write_hot_pixel_mask=self.write_hot_pixel_mask,
        )

class runSpotFinder(SpotFinder):
    def __init__(
        self,
        threshold_function=None,
        mask=None,
        region_of_interest=None,
        max_strong_pixel_fraction=0.1,
        compute_mean_background=False,
        mp_method=None,
        mp_nproc=1,
        mp_njobs=1,
        mp_chunksize=1,
        mask_generator=None,
        filter_spots=None,
        scan_range=None,
        write_hot_mask=True,
        hot_mask_prefix="hot_mask",
        min_spot_size=1,
        max_spot_size=20,
        no_shoeboxes_2d=False,
        min_chunksize=50,
        is_stills=False,
    ):
        super(runSpotFinder, self).__init__(
            threshold_function,
            mask,
            region_of_interest,
            max_strong_pixel_fraction,
            compute_mean_background,
            mp_method,
            mp_nproc,
            mp_njobs,
            mp_chunksize,
            mask_generator,
            filter_spots,
            scan_range,
            write_hot_mask,
            hot_mask_prefix,
            min_spot_size,
            max_spot_size,
            no_shoeboxes_2d,
            min_chunksize,
            is_stills,
        )
    def _find_spots_in_imageset(self, imageset):
        mask = self.mask_generator(imageset)
        """
        Do the spot finding.

        :param imageset: The imageset to process
        :return: The observed spots
        """
        # The input mask
        
        if self.mask is not None:
            mask = tuple(m1 & m2 for m1, m2 in zip(mask, self.mask))
        # Set the spot finding algorithm
        extract_spots = runExtractSpots(
            threshold_function=self.threshold_function,
            mask=mask,
            region_of_interest=self.region_of_interest,
            max_strong_pixel_fraction=self.max_strong_pixel_fraction,
            compute_mean_background=self.compute_mean_background,
            mp_method=self.mp_method,
            mp_nproc=self.mp_nproc,
            mp_njobs=self.mp_njobs,
            mp_chunksize=self.mp_chunksize,
            min_spot_size=self.min_spot_size,
            max_spot_size=self.max_spot_size,
            filter_spots=self.filter_spots,
            no_shoeboxes_2d=self.no_shoeboxes_2d,
            min_chunksize=self.min_chunksize,
            write_hot_pixel_mask=self.write_hot_mask,
        )

        # Get the max scan range
        if isinstance(imageset, ImageSequence):
            max_scan_range = imageset.get_array_range()
        else:
            max_scan_range = (0, len(imageset))

        # Get list of scan ranges
        if not self.scan_range or self.scan_range[0] is None:
            scan_range = [(max_scan_range[0] + 1, max_scan_range[1])]
        else:
            scan_range = self.scan_range

        # Get spots from bits of scan
        hot_pixels = tuple(flex.size_t() for i in range(len(imageset.get_detector())))
        reflections = flex.reflection_table()
        for j0, j1 in scan_range:
            # Make sure we were asked to do something sensible
            if j1 < j0:
                raise Sorry("Scan range must be in ascending order")
            elif j0 < max_scan_range[0] or j1 > max_scan_range[1]:
                raise Sorry(
                    "Scan range must be within image range {}..{}".format(
                        max_scan_range[0] + 1, max_scan_range[1]
                    )
                )
            logger.info("\nFinding spots in image %s to %s...", j0, j1)
            j0 -= 1
            if len(imageset) == 1:
                r, h = extract_spots(imageset)
            else:
                r, h = extract_spots(imageset[j0:j1])
            reflections.extend(r)
            if h is not None:
                for h1, h2 in zip(hot_pixels, h):
                    h1.extend(h2)
        # Find hot pixels
        hot_mask = self._create_hot_mask(imageset, hot_pixels)

        # Return as a reflection list
        return reflections, hot_mask


###############################################################################################################

start_findSpots_time = time()
script = Script()

"""Execute the script."""
# Parse the command line
# params, options = script.parser.parse_args(args=args, show_diff_phil=False)
find_spots_phil = copy.deepcopy(working_phil)
input_phil_scope = generate_input_scope(read_experiments=True)
if input_phil_scope is not None:
    find_spots_phil.adopt_scope(input_phil_scope)

params = find_spots_phil.extract()
# print(params.input.experiments)
# params.input.experiments[0] = experiments[0]
# print(params.input.experiments)



# if __name__ == "__main__":
#     # Configure the logging
#     log.config(verbosity=options.verbose, logfile=params.output.log)
# logger.info(dials_version())

# Log the diff phil
# diff_phil = script.parser.diff_phil.as_str()
# if diff_phil != "":
#     logger.info("The following parameters have been modified:\n")
#     logger.info(diff_phil)

# Ensure we have a data block
# experiments = flatten_experiments(params.input.experiments)
# print(experiments)


# did input have identifier?
had_identifiers = False
if all(i != "" for i in experiments.identifiers()):
    had_identifiers = True
    # print(had_identifiers)
else:
    generate_experiment_identifiers(
        experiments
    )  # add identifier e.g. if coming straight from images

if len(experiments) == 0:
    script.parser.print_help()
    sys.exit(0)

# print(params.maximum_trusted_value)
# If maximum_trusted_value assigned, use this temporarily for the
# spot finding
if params.maximum_trusted_value is not None:
    logger.info(
        "Overriding maximum trusted value to %.1f", params.maximum_trusted_value
    )
    input_trusted_ranges = {}
    for _d, detector in enumerate(experiments.detectors()):
        for _p, panel in enumerate(detector):
            trusted = panel.get_trusted_range()
            input_trusted_ranges[(_d, _p)] = trusted
            panel.set_trusted_range((trusted[0], params.maximum_trusted_value))

# Loop through all the imagesets and find the strong spots
# reflections = flex.reflection_table.from_observations(experiments, params)
from dials.algorithms.spot_finding.factory import SpotFinderFactory
if params is None:
    from dials.command_line.find_spots import phil_scope
    from dials.util.phil import pars
    params = phil_scope.fetch(source=parse("")).extract()

if params.spotfinder.filter.min_spot_size is libtbx.Auto:
    detector = experiments[0].imageset.get_detector()
    if detector[0].get_type() == "SENSOR_PAD":
        # smaller default value for pixel array detectors
        params.spotfinder.filter.min_spot_size = 3
    else:
         params.spotfinder.filter.min_spot_size = 6   
    logger.info(
                "Setting spotfinder.filter.min_spot_size=%i",
                params.spotfinder.filter.min_spot_size,
            )

# Get the integrator from the input parameters
logger.info("Configuring spot finder from input parameters")
# print(params.spotfinder.mp.nproc)
params.spotfinder.mp.nproc = nproc
# print(params.spotfinder.mp.nproc)
is_stills = False
from dials.algorithms.spot_finding.factory import phil_scope
if params is None:
    params = phil_scope.fetch(source=parse("")).extract()

if params.spotfinder.force_2d and params.output.shoeboxes is False:
    no_shoeboxes_2d = True
elif experiments is not None and params.output.shoeboxes is False:
    no_shoeboxes_2d = False
    all_stills = True
    for experiment in experiments:
        if isinstance(experiment.imageset, ImageSequence):
            all_stills = False
            break
        if all_stills:
            no_shoeboxes_2d = True
else:
    no_shoeboxes_2d = False
# print(no_shoeboxes_2d)
# Read in the lookup files
mask = SpotFinderFactory.load_image(params.spotfinder.lookup.mask)
params.spotfinder.lookup.mask = mask

# Configure the filter options
filter_spots = SpotFinderFactory.configure_filter(params)

# Create the threshold strategy
threshold_function = SpotFinderFactory.configure_threshold(params)
mask_generator = functools.partial(
    dials.util.masking.generate_mask, params=params.spotfinder.filter
)

# Make sure 'none' is interpreted as None
if params.spotfinder.mp.method == "none":
    params.spotfinder.mp.method = None

# from dials_spot_finder import runSpotFinder
spotfinder = runSpotFinder(
            threshold_function=threshold_function,
            mask=params.spotfinder.lookup.mask,
            filter_spots=filter_spots,
            scan_range=params.spotfinder.scan_range,
            write_hot_mask=params.spotfinder.write_hot_mask,
            hot_mask_prefix=params.spotfinder.hot_mask_prefix,
            mp_method=params.spotfinder.mp.method,
            mp_nproc=params.spotfinder.mp.nproc,
            mp_njobs=params.spotfinder.mp.njobs,
            mp_chunksize=params.spotfinder.mp.chunksize,
            max_strong_pixel_fraction=params.spotfinder.filter.max_strong_pixel_fraction,
            compute_mean_background=params.spotfinder.compute_mean_background,
            region_of_interest=params.spotfinder.region_of_interest,
            mask_generator=mask_generator,
            min_spot_size=params.spotfinder.filter.min_spot_size,
            max_spot_size=params.spotfinder.filter.max_spot_size,
            no_shoeboxes_2d=no_shoeboxes_2d,
            min_chunksize=params.spotfinder.mp.min_chunksize,
            is_stills=is_stills
        )
reflections = spotfinder.find_spots(experiments)

# Add n_signal column - before deleting shoeboxes
good = MaskCode.Foreground | MaskCode.Valid
reflections["n_signal"] = reflections["shoebox"].count_mask_values(good)

# Delete the shoeboxes
if not params.output.shoeboxes:
    del reflections["shoebox"]

# ascii spot count per image plot - per imageset

imagesets = []
for i, experiment in enumerate(experiments):
    if experiment.imageset not in imagesets:
        imagesets.append(experiment.imageset)
for i, imageset in enumerate(imagesets):
    selected = flex.bool(reflections.nrows(), False)
    for j, experiment in enumerate(experiments):
        if experiment.imageset is not imageset:
            continue
        selected.set_selected(reflections["id"] == j, True)
    ascii_plot = spot_counts_per_image_plot(reflections.select(selected))
    if len(ascii_plot):
        logger.info("\nHistogram of per-image spot count for imageset %i:", i)
        logger.info(ascii_plot)
logger.info("\n" + "-" * 80)
# If started with images and not saving experiments, then remove id mapping
# as the experiment linked to will no longer exists after exit.
if not had_identifiers:
    if not params.output.experiments:
        for k in reflections.experiment_identifiers().keys():
            del reflections.experiment_identifiers()[k]
reflections.as_file(params.output.reflections)
logger.info(
    "Saved %s reflections to %s", len(reflections), params.output.reflections)

# Reset the trusted ranges
if params.maximum_trusted_value is not None:
    for _d, detector in enumerate(experiments.detectors()):
        for _p, panel in enumerate(detector):
            trusted = input_trusted_ranges[(_d, _p)]
            panel.set_trusted_range(trusted)
# Save the experiments
if params.output.experiments:
    logger.info(f"Saving experiments to {params.output.experiments}")
    experiments.as_file(params.output.experiments)
# Print some per image statistics
if params.per_image_statistics:
    for i, experiment in enumerate(experiments):
        logger.info("Number of centroids per image for imageset %i:", i)
        refl = reflections.select(reflections["id"] == i)
        refl.centroid_px_to_mm([experiment])
        refl.map_centroids_to_reciprocal_space([experiment])
        stats = per_image_analysis.stats_per_image(
            experiment, refl, resolution_analysis=False
        )
        logger.info(str(stats))
# if params.output.experiments:
#     return experiments, reflections
# else:
#     return reflections
end_findSpots_time = time()
logger.info('Spots finding cost %s s'%(end_findSpots_time - start_findSpots_time))


###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                      DIALS  INDEX                     ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

#########################################       INDEX DEFINITION       ########################################

from dials.util.multi_dataset_handling import renumber_table_id_columns

def flatten_reflections(filename_object_list):
    """
    Flatten a list of reflections tables

    A check is also made for the 'id' values in the reflection tables, which are
    renumbered from 0..n-1 to avoid clashes. The experiment_identifiers dict is
    also updated if present in the input tables.

    :param filename_object_list: The parameter item
    :return: The flattened reflection table
    """
    tables = [filename_object_list]
    if len(tables) > 1:
        tables = renumber_table_id_columns(tables)
    return tables

def flatten_experiments(filename_object_list):
    """
    Flatten a list of experiment lists

    :param filename_object_list: The parameter item
    :return: The flattened experiment lists
    """

    result = ExperimentList()
    for o in filename_object_list:
        result.extend(o)
    return result

def reflections_and_experiments_from_files(
    reflection_file_object_list, experiment_file_object_list
):
    """Extract reflection tables and an experiment list from the files.
    If experiment identifiers are set, the order of the reflection tables is
    changed to match the order of experiments.
    """
    tables = flatten_reflections(reflection_file_object_list)

    experiments = experiment_file_object_list

    if tables and experiments:
        tables = sort_tables_to_experiments_order(tables, experiments)

    return tables, experiments

###############################################################################################################



start_index_time = time()
from dials.command_line.index import working_phil, _index_experiments, index
from dials.algorithms.indexing import DialsIndexError

index_phil = copy.deepcopy(working_phil)
input_phil_scope = generate_input_scope(read_experiments=True, read_reflections=True)
if input_phil_scope is not None:
    index_phil.adopt_scope(input_phil_scope)

params = index_phil.extract()
# all_parmas = params
# params = params.indexing
# max_lattices = params.multiple_lattice_search.max_lattices
# print(max_lattices) # 1
# d_min_step = params.refinement_protocol.d_min_step
# print(d_min_step) # Auto
# n_cycles = params.refinement_protocol.n_macro_cycles
# print(n_cycles) # 5
# mode = params.refinement_protocol.mode
# print(mode) # refine_shells
# scan_warning = all_parmas.refinement.parameterisation.scan_varying
# print(scan_warning) # False
# refinery = all_parmas.refinement.refinery
# print(refinery.engine) # LevMar

# print(len(reflections), len(experiments)) # 283597 1
# print(reflections, experiments) # 283597 1
# <dials_array_family_flex_ext.reflection_table object at 0x7faa80c1f330> 
# ExperimentList([<dxtbx_model_ext.Experiment object at 0x7faa69deddf0>])
if len(experiments) == 0:
    parser.print_help()
    sys.exit(0)
reflections, experiments = reflections_and_experiments_from_files(
        reflections, experiments
)
# params.indexing.refinement_protocol.n_macro_cycles=10
# params.indexing.basis_vector_combinations.max_refine=10
# params.indexing.nproc = params.indexing.basis_vector_combinations.max_refine
# print(params.indexing.method)
# params.indexing.method = 'fft1d'
try:
    indexed_experiments, indexed_reflections = index(
        experiments, reflections, params
    )
except (DialsIndexError, ValueError) as e:
    sys.exit(str(e))

# Save experiments
logger.info("Saving refined experiments to %s", params.output.experiments)
assert indexed_experiments.is_consistent()
indexed_experiments.as_file(params.output.experiments)
# Save reflections
logger.info("Saving refined reflections to %s", params.output.reflections)
indexed_reflections.as_file(filename=params.output.reflections)

print(indexed_reflections, indexed_experiments)
# <dials_array_family_flex_ext.reflection_table object at 0x7f97640cc830> 
# ExperimentList([<dxtbx_model_ext.Experiment object at 0x7f97670f4350>])
end_index_time = time()
logger.info("Index cost %s s"%(end_index_time - start_index_time))


###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                       DIALS  REFINE                   ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################


from dials.command_line.refine import *
"""
Set up refinement from command line options, files and PHIL parameters.
Run refinement and save output files as specified.

Called when running dials.refine as a command-line program

Args:
    args (list): Additional command-line arguments
    phil: The working PHIL parameters

Returns:
    None
"""

def flatten_reflections(filename_object_list):
    """
    Flatten a list of reflections tables

    A check is also made for the 'id' values in the reflection tables, which are
    renumbered from 0..n-1 to avoid clashes. The experiment_identifiers dict is
    also updated if present in the input tables.

    :param filename_object_list: The parameter item
    :return: The flattened reflection table
    """
    tables = [filename_object_list]
    if len(tables) > 1:
        tables = renumber_table_id_columns(tables)
    return tables

def flatten_experiments(filename_object_list):
    """
    Flatten a list of experiment lists

    :param filename_object_list: The parameter item
    :return: The flattened experiment lists
    """

    result = ExperimentList()
    for o in filename_object_list:
        result.extend(o)
    return result

def reflections_and_experiments_from_files(
    reflection_file_object_list, experiment_file_object_list
):
    """Extract reflection tables and an experiment list from the files.
    If experiment identifiers are set, the order of the reflection tables is
    changed to match the order of experiments.
    """
    tables = flatten_reflections(reflection_file_object_list)

    experiments = experiment_file_object_list

    if tables and experiments:
        tables = sort_tables_to_experiments_order(tables, experiments)

    return tables, experiments


start_refine_time = time()
refine_phil = copy.deepcopy(working_phil)
input_phil_scope = generate_input_scope(read_experiments=True, read_reflections=True)
if input_phil_scope is not None:
    refine_phil.adopt_scope(input_phil_scope)

params = refine_phil.extract()





reflections, experiments = reflections_and_experiments_from_files(
        indexed_reflections, indexed_experiments
)
# The script usage
usage = (
    "usage: dials.refine [options] [param.phil] " "models.expt observations.refl"
)
# Try to load the models and data
nexp = len(experiments)
if nexp == 0 or len(reflections) == 0:
    parser.print_help()
    sys.exit()
if len(reflections) > 1:
    sys.exit("Only one reflections list can be imported at present")
reflections = reflections[0]
# check input is suitable
msg = (
    "The supplied reflection table does not have the required data " + "column: {0}"
)
for key in ["xyzobs.mm.value", "xyzobs.mm.variance"]:
    if key not in reflections:
        sys.exit(msg.format(key))
logger.info(dials_version())
# Log the diff phil
# Warn about potentially unhelpful options
if params.refinement.mp.nproc > 1:
    logger.warning(
        "Setting nproc > 1 is only helpful in rare "
        "circumstances. It is not recommended for typical data processing "
        "tasks."
    )
if params.refinement.parameterisation.scan_varying is not False:
    # duplicate crystal if necessary for scan varying - will need
    # to compare the scans with crystals - if not 1:1 will need to
    # split the crystals
    crystal_has_scan = {}
    for j, e in enumerate(experiments):
        if e.crystal in crystal_has_scan:
            if e.scan is not crystal_has_scan[e.crystal]:
                logger.info(
                    "Duplicating crystal model for scan-varying refinement of experiment %d",
                    j,
                )
                e.crystal = copy.deepcopy(e.crystal)
        else:
            crystal_has_scan[e.crystal] = e.scan
# Run refinement
experiments, reflections, refiner, history = run_dials_refine(
    experiments, reflections, params
)
# For the usual case of refinement of one crystal, print that model for information
crystals = experiments.crystals()
if len(crystals) == 1:
    logger.info("")
    logger.info("Final refined crystal model:")
    logger.info(crystals[0])
# Write table of centroids to file, if requested
if params.output.centroids:
    logger.info(f"Writing table of centroids to '{params.output.centroids}'")
    write_centroids_table(refiner, params.output.centroids)
# Write scan-varying parameters to file, if there were any
if params.output.parameter_table:
    scans = experiments.scans()
    if len(scans) > 1:
        logger.info(
            "Writing a scan-varying parameter table is only supported "
            "for refinement of a single scan"
        )
    else:
        scan = scans[0]
        text = refiner.get_param_reporter().varying_params_vs_image_number(
            scan.get_array_range()
        )
        if text:
            logger.info(
                "Writing scan-varying parameter table to %s",
                params.output.parameter_table,
            )
            f = open(params.output.parameter_table, "w")
            f.write(text)
            f.close()
        else:
            logger.info("No scan-varying parameter table to write")
# Save the refined experiments to file
output_experiments_filename = params.output.experiments
logger.info(f"Saving refined experiments to {output_experiments_filename}")
experiments.as_file(output_experiments_filename)
# Save reflections with updated predictions if requested (allow to switch
# this off if it is a time-consuming step)
if params.output.reflections:
    logger.info(
        "Saving reflections with updated predictions to %s",
        params.output.reflections,
    )
    if params.output.include_unused_reflections:
        reflections.as_file(params.output.reflections)
    else:
        sel = reflections.get_flags(reflections.flags.used_in_refinement)
        reflections.select(sel).as_file(params.output.reflections)
# Save matches to file for debugging
if params.output.matches:
    matches = refiner.get_matches()
    logger.info(
        "Saving matches (use for debugging purposes) to %s", params.output.matches
    )
    matches.as_file(params.output.matches)
# Create correlation plots
if params.output.correlation_plot.filename is not None:
    create_correlation_plots(refiner, params.output)
# Save refinement history
if params.output.history:
    logger.info(f"Saving refinement step history to {params.output.history}")
    history.to_json_file(params.output.history)
refine_experiments = experiments
refine_reflections = reflections
end_refine_time = time()
logger.info("Refine cost %s s"%(end_refine_time - start_refine_time))

###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################

###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                     DIALS  INTEGRATE                  ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

#########################################     INTEGRATE DEFINITION     ########################################


from dials.command_line.integrate import *

from dials.algorithms.integration.integrator import *
from dials.algorithms.integration.integrator import _initialize_rotation, _finalize_rotation, Parameters
from dials.algorithms.integration.processor import _Manager, _Processor, build_processor
from dials.algorithms.profile_model.factory import ProfileModelFactory
from dials.algorithms.integration.report import IntegrationReport
from dials.util import Sorry
from dials.array_family import flex
from dials_algorithms_integration_integrator_ext import JobList
import libtbx
import random
import pickle
import itertools

class runProcessor(_Processor):
    """Processor interface class."""

    def __init__(self, manager):
        """
        Initialise the processor.

        The processor requires a manager class implementing the _Manager interface.
        This class executes all the workers in separate threads and accumulates the
        results to expose to the user.

        :param manager: The processing manager
        :param params: The phil parameters
        """
        self.manager = manager


    def process(self):
        """
        Do all the processing tasks.

        :return: The processing results
        """
        start_time = time()
        self.manager.initialize()
        print(len((self.manager.jobs)))
        print(len(self.manager))
        # mp_method = self.manager.params.mp.method
        # mp_njobs = self.manager.params.mp.njobs
        # mp_nproc = self.manager.params.mp.nproc

        # assert mp_nproc > 0, "Invalid number of processors"
        # if mp_nproc * mp_njobs > len(self.manager):
        #     mp_nproc = min(mp_nproc, len(self.manager))
        #     mp_njobs = int(math.ceil(len(self.manager) / mp_nproc))
        # logger.info(self.manager.summary())
        # if mp_njobs > 1:
        #     assert mp_method != "none" and mp_method is not None
        #     logger.info(
        #         " Using %s with %d parallel job(s) and %d processes per node\n",
        #         mp_method,
        #         mp_njobs,
        #         mp_nproc,
        #     )
        # else:
        #     logger.info(" Using multiprocessing with %d parallel job(s)\n", mp_nproc)

        # if mp_njobs * mp_nproc > 1:

        #     def process_output(result):
        #         rehandle_cached_records(result[1])
        #         self.manager.accumulate(result[0])

        #     multi_node_parallel_map(
        #         func=execute_parallel_task,
        #         iterable=list(self.manager.tasks()),
        #         njobs=mp_njobs,
        #         nproc=mp_nproc,
        #         callback=process_output,
        #         cluster_method=mp_method,
        #         preserve_order=True,
        #     )
        # else:
        #     for task in self.manager.tasks():
        #         self.manager.accumulate(task())
        task_list = [(i, self.manager.task(i)) for i in range(len(self.manager))]
        # indices = list(range(len(self.manager)))
        integrateRDD = sc.parallelize(task_list, len(self.manager))
        after  = integrateRDD.map(lambda x : (x[0], x[1]())).collect()
        sorted(after, key = lambda tup: tup[0])
        for i, result in after:
            self.manager.accumulate(result)
        self.manager.finalize()
        end_time = time()
        self.manager.time.user_time = end_time - start_time
        result1, result2 = self.manager.result()
        return result1, result2, self.manager.time


class runProcessorRot(runProcessor):
    """Processor interface class for rotation data only."""

    def __init__(self, experiments, manager):
        """
        Initialise the processor.

        The processor requires a manager class implementing the _Manager interface.
        This class executes all the workers in separate threads and accumulates the
        results to expose to the user.

        :param manager: The processing manager
        """
        # Ensure we have the correct type of data
        if not experiments.all_sequences():
            raise RuntimeError(
                """
        An inappropriate processing algorithm may have been selected!
         Trying to perform rotation processing when not all experiments
         are indicated as rotation experiments.
      """
            )

        super().__init__(manager)


class runManager(_Manager):
    def __init__(self, experiments, reflections, params):
        """
        Initialise the manager.

        :param experiments: The list of experiments
        :param reflections: The list of reflections
        :param params: The phil parameters
        """

        # Initialise the callbacks
        self.executor = None

        # Save some data
        self.experiments = experiments
        self.reflections = reflections

        # Other data
        self.data = {}

        # Save some parameters
        self.params = params

        # Set the finalized flag to False
        self.finalized = False

        # Initialise the timing information
        self.time = dials.algorithms.integration.TimingInfo()

    def compute_jobs(self):
        """
        Sets up a JobList() object in self.jobs
        """

        if self.params.block.size == libtbx.Auto:
            if (
                self.params.mp.nproc * self.params.mp.njobs == 1
                and not self.params.debug.output
                and not self.params.block.force
            ):
                self.params.block.size = None

        # calculate the block overlap based on the size of bboxes in the data
        # calculate once here rather than repeated in the loop below
        block_overlap = 0
        if self.params.block.size is not None:
            assert self.params.block.threshold > 0, "Threshold must be > 0"
            assert self.params.block.threshold <= 1.0, "Threshold must be < 1"
            frames_per_refl = sorted([b[5] - b[4] for b in self.reflections["bbox"]])
            cutoff = int(self.params.block.threshold * len(frames_per_refl))
            block_overlap = frames_per_refl[cutoff]
        print(block_overlap)
        groups = itertools.groupby(
            range(len(self.experiments)),
            lambda x: (id(self.experiments[x].imageset), id(self.experiments[x].scan)),
        )
        self.jobs = JobList()
        for key, indices in groups:
            indices = list(indices)
            i0 = indices[0]
            i1 = indices[-1] + 1
            expr = self.experiments[i0]
            scan = expr.scan
            imgs = expr.imageset
            array_range = (0, len(imgs))
            if scan is not None:
                assert len(imgs) >= len(scan), "Invalid scan range"
                array_range = scan.get_array_range()

            if self.params.block.size is None:
                block_size_frames = array_range[1] - array_range[0]
            elif self.params.block.size == libtbx.Auto:
                # auto determine based on nframes and overlap
                nframes = array_range[1] - array_range[0]
                print(nframes)
                nblocks = self.params.mp.nproc * self.params.mp.njobs
                print(nblocks)
                # want data to be split into n blocks with overlaps
                # i.e. [x, overlap, y, overlap, y, overlap, ....,y,  overlap, x]
                # blocks are x + overlap, or overlap + y + overlap.
                x = (nframes - block_overlap) / nblocks
                print(x)
                block_size = int(math.ceil(x + block_overlap))
                print(block_size)
                # increase the block size to be at least twice the overlap, in
                # case the overlap is large e.g. if high mosaicity.
                block_size_frames = max(block_size, 2 * block_overlap)
            elif self.params.block.units == "radians":
                _, dphi = scan.get_oscillation(deg=False)
                block_size_frames = int(math.ceil(self.params.block.size / dphi))
                # if the specified block size is lower than the overlap,
                # reduce the overlap to be half of the block size.
                block_overlap = min(block_overlap, int(block_size_frames // 2))
            elif self.params.block.units == "degrees":
                _, dphi = scan.get_oscillation()
                block_size_frames = int(math.ceil(self.params.block.size / dphi))
                # if the specified block size is lower than the overlap,
                # reduce the overlap to be half of the block size.
                block_overlap = min(block_overlap, int(block_size_frames // 2))
            elif self.params.block.units == "frames":
                block_size_frames = int(math.ceil(self.params.block.size))
                block_overlap = min(block_overlap, int(block_size_frames // 2))
            else:
                raise RuntimeError(
                    f"Unknown block_size units {self.params.block.units!r}"
                )
            self.jobs.add(
                (i0, i1),
                array_range,
                block_size_frames,
                block_overlap,
            )
        assert len(self.jobs) > 0, "Invalid number of jobs"
    
    # def compute_jobs(self):
    #     """
    #     Sets up a JobList() object in self.jobs
    #     """
    #     # global nproc
    #     if self.params.block.size == libtbx.Auto:
    #         if (
    #             self.params.mp.nproc * self.params.mp.njobs == 1
    #             and not self.params.debug.output
    #             and not self.params.block.force
    #         ):
    #             self.params.block.size = None

    #     # calculate the block overlap based on the size of bboxes in the data
    #     # calculate once here rather than repeated in the loop below
    #     # self.params.mp.nproc = nproc
    #     block_overlap = 1
    #     if self.params.block.size is not None:
    #         print('TEST 1')
    #         assert self.params.block.threshold > 0, "Threshold must be > 0"
    #         assert self.params.block.threshold <= 1.0, "Threshold must be < 1"
    #         frames_per_refl = sorted([b[5] - b[4] for b in self.reflections["bbox"]])
    #         cutoff = int(self.params.block.threshold * len(frames_per_refl))
    #         block_overlap = frames_per_refl[cutoff]
    #     print(block_overlap)
    #     # block_overlap = 1
    #     groups = itertools.groupby(
    #         range(len(self.experiments)),
    #         lambda x: (id(self.experiments[x].imageset), id(self.experiments[x].scan)),
    #     )
    #     self.jobs = JobList()
    #     for key, indices in groups:
    #         indices = list(indices)
    #         i0 = indices[0]
    #         i1 = indices[-1] + 1
    #         expr = self.experiments[i0]
    #         scan = expr.scan
    #         imgs = expr.imageset
    #         array_range = (0, len(imgs))
    #         if scan is not None:
    #             print('TEST 2')
    #             assert len(imgs) >= len(scan), "Invalid scan range"
    #             array_range = scan.get_array_range()

    #         if self.params.block.size is None:
    #             print('TEST 3')
    #             block_size_frames = array_range[1] - array_range[0]
    #         elif self.params.block.size == libtbx.Auto:
    #             print('TEST 4')
    #             # auto determine based on nframes and overlap
    #             nframes = array_range[1] - array_range[0]
    #             print(nframes)
    #             nblocks = self.params.mp.nproc * self.params.mp.njobs
    #             # print(nblocks)
    #             # want data to be split into n blocks with overlaps
    #             # i.e. [x, overlap, y, overlap, y, overlap, ....,y,  overlap, x]
    #             # blocks are x + overlap, or overlap + y + overlap.
    #             x = (nframes - block_overlap) / nblocks
    #             block_size = int(math.ceil(x + block_overlap))
    #             # print(x)
    #             # block_size = 3
    #             print(block_size)
    #             # increase the block size to be at least twice the overlap, in
    #             # case the overlap is large e.g. if high mosaicity.
    #             block_size_frames = max(block_size, 2 * block_overlap)
    #         elif self.params.block.units == "radians":
    #             print('TEST 5')
    #             _, dphi = scan.get_oscillation(deg=False)
    #             block_size_frames = int(math.ceil(self.params.block.size / dphi))
    #             # if the specified block size is lower than the overlap,
    #             # reduce the overlap to be half of the block size.
    #             block_overlap = min(block_overlap, int(block_size_frames // 2))
    #         elif self.params.block.units == "degrees":
    #             print('TEST 6')
    #             _, dphi = scan.get_oscillation()
    #             block_size_frames = int(math.ceil(self.params.block.size / dphi))
    #             # if the specified block size is lower than the overlap,
    #             # reduce the overlap to be half of the block size.
    #             block_overlap = min(block_overlap, int(block_size_frames // 2))
    #         elif self.params.block.units == "frames":
    #             print('TEST 7')
    #             block_size_frames = int(math.ceil(self.params.block.size))
    #             block_overlap = min(block_overlap, int(block_size_frames // 2))
    #         else:
    #             print('TEST 8')
    #             raise RuntimeError(
    #                 f"Unknown block_size units {self.params.block.units!r}"
    #             )
    #         print(array_range, block_size_frames, block_overlap)
    #         self.jobs.add(
    #             (i0, i1),
    #             array_range,
    #             block_size_frames,
    #             block_overlap,
    #         )
    #     assert len(self.jobs) > 0, "Invalid number of jobs"
    #     print(len(self.jobs))

class runProcessor3D(runProcessorRot):
    """Top level processor for 3D processing."""

    def __init__(self, experiments, reflections, params):
        """Initialise the manager and the processor."""

        # Set some parameters
        params.shoebox.partials = False
        params.shoebox.flatten = False

        # Create the processing manager
        manager = runManager(experiments, reflections, params)

        # Initialise the processor
        super().__init__(experiments, manager)


class runIntegrator(Integrator):
    """
    The integrator class
    """

    def __init__(self, experiments, reflections, params):
        """
        Initialize the integrator

        :param experiments: The experiment list
        :param reflections: The reflections to process
        :param params: The parameters to use
        """

        # Save some stuff
        self.experiments = experiments
        self.reflections = reflections
        self.params = Parameters.from_phil(params.integration)
        self.profile_model_report = None
        self.integration_report = None

    def integrate(self):
        """
        Integrate the data
        """
        # Ensure we get the same random sample each time
        random.seed(0)

        # Init the report
        self.profile_model_report = None
        self.integration_report = None

        # Heading
        logger.info("=" * 80)
        logger.info("")
        logger.info(heading("Processing reflections"))
        logger.info("")

        # Print the summary
        logger.info(
            " Processing the following experiments:\n"
            "\n"
            " Experiments: %d\n"
            " Beams:       %d\n"
            " Detectors:   %d\n"
            " Goniometers: %d\n"
            " Scans:       %d\n"
            " Crystals:    %d\n"
            " Imagesets:   %d\n",
            len(self.experiments),
            len(self.experiments.beams()),
            len(self.experiments.detectors()),
            len(self.experiments.goniometers()),
            len(self.experiments.scans()),
            len(self.experiments.crystals()),
            len(self.experiments.imagesets()),
        )

        # Initialize the reflections
        self.initialize_reflections(self.experiments, self.params, self.reflections)

        # Check if we want to do some profile fitting
        profile_fitter = self.fit_profiles()

        logger.info("=" * 80)
        logger.info("")
        logger.info(heading("Integrating reflections"))
        logger.info("")

        # Create the data processor
        executor = IntegratorExecutor(
            self.experiments,
            profile_fitter,
            self.params.profile.valid_foreground_threshold,
        )

        # determine the max memory needed during integration
        def _determine_max_memory_needed(experiments, reflections):
            max_needed = 0
            for imageset in experiments.imagesets():
                # find all experiments belonging to that imageset, as each
                # imageset is processed as a whole for integration.
                if all(experiments.identifiers()):
                    expt_ids = [
                        experiment.identifier
                        for experiment in experiments
                        if experiment.imageset == imageset
                    ]
                    subset = reflections.select_on_experiment_identifiers(expt_ids)
                else:
                    subset = flex.reflection_table()
                    for j, experiment in enumerate(experiments):
                        if experiment.imageset == imageset:
                            subset.extend(reflections.select(reflections["id"] == j))
                try:
                    if imageset.get_scan():
                        frame0, frame1 = imageset.get_scan().get_array_range()
                    else:
                        raise RuntimeError
                except RuntimeError:  # catch DXTBX_ASSERT if no scan in imageset
                    frame0, frame1 = (0, len(imageset))
                flatten = self.params.integration.integrator == "flat3d"
                max_needed = max(
                    max_memory_needed(subset, frame0, frame1, flatten),
                    max_needed,
                )
            assert max_needed > 0, "Could not determine memory requirements"
            return max_needed

        def _iterative_table_split(tables, experiments, available_memory):
            split_tables = []
            for table in tables:
                mem_needed = _determine_max_memory_needed(experiments, table)
                if mem_needed > available_memory:
                    n_to_split = int(math.ceil(mem_needed / available_memory))
                    flex.set_random_seed(0)
                    split_tables.extend(table.random_split(n_to_split))
                else:
                    split_tables.append(table)
            if len(split_tables) == len(tables):
                # nothing was split, all passed memory check
                return split_tables
            # some tables were split - so need to check again that all are ok
            return _iterative_table_split(split_tables, experiments, available_memory)

        def _run_processor(reflections):
            processor = build_processor(
                self.ProcessorClass,
                self.experiments,
                reflections,
                self.params.integration,
            )
            processor.executor = executor
            # Process the reflections
            reflections, _, time_info = processor.process()
            return reflections, time_info

        # if self.params.integration.mp.method != "multiprocessing":
        self.reflections, time_info = _run_processor(self.reflections)
#         else:
#             # need to do a memory check and decide whether to split table
#             available_immediate, _, __ = assess_available_memory(
#                 self.params.integration
#             )

#             #  here don't consider nproc as the processor will reduce nproc to 1
#             # if necessary, only want to split if we can't even process with
#             # nproc = 1

#             if self.params.integration.mp.n_subset_split:
#                 tables = self.reflections.random_split(
#                     self.params.integration.mp.n_subset_split
#                 )
#             else:
#                 tables = _iterative_table_split(
#                     [self.reflections],
#                     self.experiments,
#                     available_immediate,
#                 )

#             if len(tables) == 1:
#                 # will not fail a memory check in the processor, so proceed
#                 self.reflections, time_info = _run_processor(self.reflections)
#             else:
#                 # Split the reflections and process by performing multiple
#                 # passes over each imageset
#                 time_info = TimingInfo()
#                 reflections = flex.reflection_table()

#                 logger.info(
#                     """Predicted maximum memory needed exceeds available memory.
# Splitting reflection table into %s subsets for processing
# """,
#                     len(tables),
#                 )
#                 for i, table in enumerate(tables):
#                     logger.info("Processing subset %s of reflection table", i + 1)
#                     processed, this_time_info = _run_processor(table)
#                     reflections.extend(processed)
#                     time_info += this_time_info
#                 self.reflections = reflections

        # Finalize the reflections
        self.reflections, self.experiments = self.finalize_reflections(
            self.reflections, self.experiments, self.params
        )

        # Create the integration report
        self.integration_report = IntegrationReport(self.experiments, self.reflections)
        logger.info("")
        logger.info(self.integration_report.as_str(prefix=" "))

        # Print the time info
        logger.info("Timing information for integration")
        logger.info(str(time_info))
        logger.info("")

        # Return the reflections
        return self.reflections

class runIntegrator3D(runIntegrator):
    initialize_reflections = staticmethod(_initialize_rotation)
    ProcessorClass = runProcessor3D
    finalize_reflections = staticmethod(_finalize_rotation)


def create_integrator(params, experiments, reflections):
    """
    Create an integrator object with a given configuration.

    :param params: The input phil parameters
    :param experiments: The list of experiments
    :param reflections: The reflections to integrate
    :return: An integrator object
    """
    # Check each experiment has an imageset
    for exp in experiments:
        if exp.imageset is None:
            raise Sorry(
                """
      One or more experiment does not contain an imageset. Access to the
      image data is crucial for integration.
    """
            )

    # Read the mask in if necessary
    if params.integration.lookup.mask and isinstance(
        params.integration.lookup.mask, str
    ):
        with open(params.integration.lookup.mask, "rb") as infile:
            params.integration.lookup.mask = pickle.load(infile, encoding="bytes")

    # Set algorithms as reflection table defaults
    BackgroundAlgorithm = dials.extensions.Background.load(
        params.integration.background.algorithm
    )
    flex.reflection_table.background_algorithm = functools.partial(
        BackgroundAlgorithm, params
    )
    CentroidAlgorithm = dials.extensions.Centroid.load(
        params.integration.centroid.algorithm
    )
    flex.reflection_table.centroid_algorithm = functools.partial(
        CentroidAlgorithm, params
    )

    # Get the classes we need
    if params.integration.integrator == "auto":
        if experiments.all_stills():
            params.integration.integrator = "stills"
        else:
            params.integration.integrator = "3d"
    IntegratorClass = {
        "3d": runIntegrator3D,
        "flat3d": IntegratorFlat3D,
        "2d": Integrator2D,
        "single2d": IntegratorSingle2D,
        "stills": IntegratorStills,
        "3d_threaded": Integrator3DThreaded,
    }.get(params.integration.integrator)
    if not IntegratorClass:
        raise ValueError(f"Unknown integration type {params.integration.integrator}")

    # Remove scan if stills
    if experiments.all_stills():
        for experiment in experiments:
            experiment.scan = None

    # Return an instantiation of the class
    return IntegratorClass(experiments, reflections, params)

def flatten_reflections(filename_object_list):
    """
    Flatten a list of reflections tables

    A check is also made for the 'id' values in the reflection tables, which are
    renumbered from 0..n-1 to avoid clashes. The experiment_identifiers dict is
    also updated if present in the input tables.

    :param filename_object_list: The parameter item
    :return: The flattened reflection table
    """
    tables = [filename_object_list]
    if len(tables) > 1:
        tables = renumber_table_id_columns(tables)
    return tables

def flatten_experiments(filename_object_list):
    """
    Flatten a list of experiment lists

    :param filename_object_list: The parameter item
    :return: The flattened experiment lists
    """

    result = ExperimentList()
    for o in filename_object_list:
        result.extend(o)
    return result

def reflections_and_experiments_from_files(
    reflection_file_object_list, experiment_file_object_list
):
    """Extract reflection tables and an experiment list from the files.
    If experiment identifiers are set, the order of the reflection tables is
    changed to match the order of experiments.
    """
    tables = flatten_reflections(reflection_file_object_list)

    experiments = experiment_file_object_list

    if tables and experiments:
        tables = sort_tables_to_experiments_order(tables, experiments)

    return tables, experiments


###############################################################################################################
start_integrate_time = time()
integrate_phil = copy.deepcopy(working_phil)
input_phil_scope = generate_input_scope(read_experiments=True, read_reflections=True)
if input_phil_scope is not None:
    integrate_phil.adopt_scope(input_phil_scope)

params = integrate_phil.extract()

params.integration.mp.nproc = nproc



reference, experiments = reflections_and_experiments_from_files(
        refine_reflections, refine_experiments
)
if not reference and not experiments:
    parser.print_help()
    sys.exit(0)
if not experiments:
    sys.exit("No experiment list was specified")
if not reference:
    reference = None
elif len(reference) != 1:
    sys.exit("More than 1 reflection file was given")
else:
    reference = reference[0]
if reference and "shoebox" not in reference:
    sys.exit("Error: shoebox data missing from reflection table")
try:
    predicted = None
    rubbish = None

    for abs_params in params.absorption_correction:
        if abs_params.apply:
            if not (
                params.integration.debug.output
                and not params.integration.debug.separate_files
            ):
                raise ValueError(
                    "Shoeboxes must be saved to integration intermediates to apply an absorption correction. "
                    + "Set integration.debug.output=True, integration.debug.separate_files=False and "
                    + "integration.debug.delete_shoeboxes=True to temporarily store shoeboxes."
                )

    # Print if we're using a mask
    for i, exp in enumerate(experiments):
        mask = exp.imageset.external_lookup.mask
        if mask.filename is not None:
            if mask.data:
                logger.info("Using external mask: %s", mask.filename)
                for tile in mask.data:
                    logger.info(" Mask has %d pixels masked", tile.data().count(False))

    # Print the experimental models
    for i, exp in enumerate(experiments):
        summary = "\n".join(
            (
                "",
                "=" * 80,
                "",
                "Experiments",
                "",
                "Models for experiment %d" % i,
                "",
                str(exp.beam),
                str(exp.detector),
            )
        )
        if exp.goniometer:
            summary += str(exp.goniometer) + "\n"
        if exp.scan:
            summary += str(exp.scan) + "\n"
        summary += str(exp.crystal)
        logger.info(summary)

    logger.info("\n".join(("", "=" * 80, "")))
    logger.info(heading("Initialising"))

    # Load the data
    if reference:
        reference, rubbish = process_reference(reference)

        # Check pixels don't belong to neighbours
        if exp.goniometer is not None and exp.scan is not None:
            reference = filter_reference_pixels(reference, experiments)

        # Modify experiment list if scan range is set.
        experiments, reference = split_for_scan_range(
            experiments, reference, params.scan_range
        )

    # Modify experiment list if exclude images is set
    if params.exclude_images:
        for experiment in experiments:
            for index in params.exclude_images:
                experiment.imageset.mark_for_rejection(index, True)

    # Predict the reflections
    logger.info("\n".join(("", "=" * 80, "")))
    logger.info(heading("Predicting reflections"))
    predicted = flex.reflection_table.from_predictions_multi(
        experiments,
        dmin=params.prediction.d_min,
        dmax=params.prediction.d_max,
        margin=params.prediction.margin,
        force_static=params.prediction.force_static,
        padding=params.prediction.padding,
    )
    isets = OrderedSet(e.imageset for e in experiments)
    predicted["imageset_id"] = flex.int(predicted.size(), 0)
    if len(isets) > 1:
        for e in experiments:
            iset_id = isets.index(e.imageset)
            for id_ in predicted.experiment_identifiers().keys():
                identifier = predicted.experiment_identifiers()[id_]
                if identifier == e.identifier:
                    sel = predicted["id"] == id_
                    predicted["imageset_id"].set_selected(sel, iset_id)
                    break

    # Match reference with predicted
    if reference:
        matched, reference, unmatched = predicted.match_with_reference(reference)
        assert len(matched) == len(predicted)
        assert matched.count(True) <= len(reference)
        if matched.count(True) == 0:
            raise ValueError(
                """
        Invalid input for reference reflections.
        Zero reference spots were matched to predictions
    """
            )
        elif unmatched:
            msg = (
                "Warning: %d reference spots were not matched to predictions"
                % unmatched.size()
            )
            border = "\n".join(("", "*" * 80, ""))
            logger.info("".join((border, msg, border)))
            rubbish.extend(unmatched)

        if len(experiments) > 1:
            # filter out any experiments without matched reference reflections
            # f_: filtered

            f_reference = flex.reflection_table()
            f_predicted = flex.reflection_table()
            f_rubbish = flex.reflection_table()
            f_experiments = ExperimentList()
            good_expt_count = 0

            def refl_extend(src, dest, eid):
                old_id = eid
                new_id = good_expt_count
                tmp = src.select(src["id"] == old_id)
                tmp["id"] = flex.int(len(tmp), good_expt_count)
                if old_id in tmp.experiment_identifiers():
                    identifier = tmp.experiment_identifiers()[old_id]
                    del tmp.experiment_identifiers()[old_id]
                    tmp.experiment_identifiers()[new_id] = identifier
                dest.extend(tmp)

            for expt_id, experiment in enumerate(experiments):
                if len(reference.select(reference["id"] == expt_id)) != 0:
                    refl_extend(reference, f_reference, expt_id)
                    refl_extend(predicted, f_predicted, expt_id)
                    refl_extend(rubbish, f_rubbish, expt_id)
                    f_experiments.append(experiment)
                    good_expt_count += 1
                else:
                    logger.info(
                        "Removing experiment %d: no reference reflections matched to predictions",
                        expt_id,
                    )

            reference = f_reference
            predicted = f_predicted
            experiments = f_experiments
            rubbish = f_rubbish

    # Select a random sample of the predicted reflections
    if not params.sampling.integrate_all_reflections:
        predicted = sample_predictions(experiments, predicted, params)

    # Compute the profile model - either load existing or compute
    # can raise RuntimeError
    experiments = ProfileModelFactory.create(params, experiments, reference)
    for expr in experiments:
        if expr.profile is None:
            raise ValueError("No profile information in experiment list")
    del reference

    # Compute the bounding box
    predicted.compute_bbox(experiments)

    # Create the integrator
    integrator = create_integrator(params, experiments, predicted)
    # print(integrator)

    # # Integrate the reflections
    # reflections = integrator.integrate()

    # Check each experiment has an imageset
    # for exp in experiments:
    #     if exp.imageset is None:
    #         raise Sorry(
    #             """
    #   One or more experiment does not contain an imageset. Access to the
    #   image data is crucial for integration.
    # """
    #         )
        
    # if params.integration.lookup.mask and isinstance(
    #     params.integration.lookup.mask, str
    # ):
    #     # print("TEST 1")
    #     with open(params.integration.lookup.mask, "rb") as infile:
    #         params.integration.lookup.mask = pickle.load(infile, encoding="bytes")

    # # Set algorithms as reflection table defaults
    # BackgroundAlgorithm = dials.extensions.Background.load(
    #     params.integration.background.algorithm
    # )
    # flex.reflection_table.background_algorithm = functools.partial(
    #     BackgroundAlgorithm, params
    # )
    # CentroidAlgorithm = dials.extensions.Centroid.load(
    #     params.integration.centroid.algorithm
    # )
    # flex.reflection_table.centroid_algorithm = functools.partial(
    #     CentroidAlgorithm, params
    # )

    # # Get the classes we need
    # if params.integration.integrator == "auto":
    #     if experiments.all_stills():
    #         # print("TEST 2")
    #         params.integration.integrator = "stills"
    #     else:
    #         # print("TEST 3") # HERE
    #         params.integration.integrator = "3d"
    # IntegratorClass = {
    #     "3d": Integrator3D,
    #     "flat3d": IntegratorFlat3D,
    #     "2d": Integrator2D,
    #     "single2d": IntegratorSingle2D,
    #     "stills": IntegratorStills,
    #     "3d_threaded": Integrator3DThreaded,
    # }.get(params.integration.integrator)
    # if not IntegratorClass:
    #     raise ValueError(f"Unknown integration type {params.integration.integrator}")
    # # print(IntegratorClass)  # <class 'dials.algorithms.integration.integrator.Integrator3D'>
    # # Remove scan if stills
    # if experiments.all_stills():
    #     for experiment in experiments:
    #         experiment.scan = None
    # integrator = Integrator3D(experiments, predicted, params)
    # # print(len(integrator.reflections))

    reflections = integrator.integrate()
    # Remove unintegrated reflections
    if not params.output.output_unintegrated_reflections:
        keep = reflections.get_flags(reflections.flags.integrated, all=False)
        logger.info(
            "Removing %d unintegrated reflections of %d total",
            keep.count(False),
            keep.size(),
        )

        reflections = reflections.select(keep)

    # Append rubbish data onto the end
    if rubbish is not None and params.output.include_bad_reference:
        mask = flex.bool(len(rubbish), True)
        rubbish.unset_flags(mask, rubbish.flags.integrated_sum)
        rubbish.unset_flags(mask, rubbish.flags.integrated_prf)
        rubbish.set_flags(mask, rubbish.flags.bad_reference)
        reflections.extend(rubbish)

    # Correct integrated intensities for absorption correction, if necessary
    for abs_params in params.absorption_correction:
        if abs_params.apply and abs_params.algorithm == "fuller_kapton":
            from dials.algorithms.integration.kapton_correction import (
                multi_kapton_correction,
            )

            experiments, reflections = multi_kapton_correction(
                experiments, reflections, abs_params.fuller_kapton, logger=logger
            )()

    if params.significance_filter.enable:
        from dials.algorithms.integration.stills_significance_filter import (
            SignificanceFilter,
        )

        sig_filter = SignificanceFilter(params)
        filtered_refls = sig_filter(experiments, reflections)
        accepted_expts = ExperimentList()
        accepted_refls = flex.reflection_table()
        logger.info(
            "Removed %d reflections out of %d when applying significance filter",
            (reflections.size() - filtered_refls.size()),
            reflections.size(),
        )
        for expt_id, expt in enumerate(experiments):
            refls = filtered_refls.select(filtered_refls["id"] == expt_id)
            if refls:
                accepted_expts.append(expt)
                current_id = expt_id
                new_id = len(accepted_expts) - 1
                refls["id"] = flex.int(len(refls), new_id)
                if expt.identifier:
                    del refls.experiment_identifiers()[current_id]
                    refls.experiment_identifiers()[new_id] = expt.identifier
                accepted_refls.extend(refls)
            else:
                logger.info(
                    "Removed experiment %d which has no reflections left after applying significance filter",
                    expt_id,
                )

        if not accepted_refls:
            raise ValueError("No reflections left after applying significance filter")
        experiments = accepted_expts
        reflections = accepted_refls

    # Write a report if requested
    report = None
    if params.output.report is not None:
        report = integrator.report()


except (ValueError, RuntimeError) as e:
        sys.exit(e)

else:
    # Delete the shoeboxes used for intermediate calculations, if requested
    if params.integration.debug.delete_shoeboxes and "shoebox" in reflections:
        del reflections["shoebox"]

    logger.info(
        "Saving %d reflections to %s", reflections.size(), params.output.reflections
    )
    reflections.as_file(params.output.reflections)
    logger.info("Saving the experiments to %s", params.output.experiments)
    experiments.as_file(params.output.experiments)

    if report:
        report.as_file(params.output.report)

# print(reflections, experiments)
# <dials_array_family_flex_ext.reflection_table object at 0x7f73552a6e30> 
# ExperimentList([<dxtbx_model_ext.Experiment object at 0x7f736f4ab760>])
end_integrate_time = time()
logger.info("Integrate cost %s s"%(end_integrate_time - start_integrate_time))
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                      DIALS  SYMMETRY                  ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

#########################################     SYMMETRY DEFINITION      ########################################

from dials.command_line.symmetry import  phil_scope, \
    change_of_basis_ops_to_minimum_cell, \
    eliminate_sys_absent, \
    apply_change_of_basis_ops, \
    get_subset_for_symmetry, \
    _reindex_experiments_reflections
import logging
# from dials.util.options import ArgumentParser, reflections_and_experiments_from_files
from dials.array_family import flex
from dials.util.multi_dataset_handling import (
    assign_unique_identifiers,
    parse_multiple_datasets,
    # update_imageset_ids,
)
from libtbx import Auto
from dials.algorithms.symmetry.absences.screw_axes import ScrewAxisObserver
from dials.algorithms.symmetry.absences.laue_groups_info import (
    laue_groups as laue_groups_for_absence_analysis,
)
from dials.algorithms.symmetry.absences.run_absences_checks import (
    run_systematic_absences_checks,
)
from dials.algorithms.merging.merge import prepare_merged_reflection_table
from dials.algorithms.symmetry import resolution_filter_from_reflections_experiments
# from dials.util import log, tabulate
from dials.algorithms.symmetry.laue_group import LaueGroupAnalysis
from dials.util.filter_reflections import filtered_arrays_from_experiments_reflections
# import random
# import sys
import json

###############################################################################################################
start_symmetry_time = time()
symmetry_phil = copy.deepcopy(phil_scope)
input_phil_scope = generate_input_scope(read_experiments=True, read_reflections=True)
if input_phil_scope is not None:
    symmetry_phil.adopt_scope(input_phil_scope)

params = symmetry_phil.extract()
if params.seed is not None:
    flex.set_random_seed(params.seed)
    random.seed(params.seed)

reflections, experiments = reflections_and_experiments_from_files(
    reflections, experiments
)
reflections = parse_multiple_datasets(reflections)

if len(experiments) != len(reflections):
    sys.exit(
        "Mismatched number of experiments and reflection tables found: %s & %s."
        % (len(experiments), len(reflections))
    )

try:
    experiments, reflection_tables = assign_unique_identifiers(experiments, reflections)
    """
    Run symmetry analysis

    Args:
        experiments: An experiment list.
        reflection_tables: A list of reflection tables.
        params: The dials.symmetry phil scope.
    """
    result = None
    if params is None:
        params = phil_scope.extract()
    refls_for_sym = []

    if params.laue_group is Auto:
        logger.info("=" * 80)
        logger.info("")
        logger.info("Performing Laue group analysis")
        logger.info("")

        # Transform models into miller arrays
        n_datasets = len(experiments)

        # Map experiments and reflections to minimum cell
        # Eliminate reflections that are systematically absent due to centring
        # of the lattice, otherwise they would lead to non-integer miller indices
        # when reindexing to a primitive setting
        cb_ops = change_of_basis_ops_to_minimum_cell(
            experiments,
            params.lattice_symmetry_max_delta,
            params.relative_length_tolerance,
            params.absolute_angle_tolerance,
        )
        reflection_tables = eliminate_sys_absent(experiments, reflection_tables)
        experiments, reflection_tables = apply_change_of_basis_ops(
            experiments, reflection_tables, cb_ops
        )
        params.exclude_images = []
        print(type(params.exclude_images))
        print(params.exclude_images)
        refls_for_sym = get_subset_for_symmetry(
            experiments, reflection_tables, params.exclude_images
        )

        datasets = filtered_arrays_from_experiments_reflections(
            experiments,
            refls_for_sym,
            outlier_rejection_after_filter=True,
            partiality_threshold=params.partiality_threshold,
        )
        if len(datasets) != n_datasets:
            raise ValueError(
                """Some datasets have no reflection after prefiltering, please check
    input data and filtering settings e.g partiality_threshold"""
            )

        datasets = [
            ma.as_anomalous_array().merge_equivalents().array() for ma in datasets
        ]
        result = LaueGroupAnalysis(
            datasets,
            normalisation=params.normalisation,
            d_min=params.d_min,
            min_i_mean_over_sigma_mean=params.min_i_mean_over_sigma_mean,
            lattice_symmetry_max_delta=params.lattice_symmetry_max_delta,
            relative_length_tolerance=params.relative_length_tolerance,
            absolute_angle_tolerance=params.absolute_angle_tolerance,
            best_monoclinic_beta=params.best_monoclinic_beta,
        )
        logger.info("")
        logger.info(result)

        if params.output.json is not None:
            d = result.as_dict()
            d["cb_op_inp_min"] = [str(cb_op) for cb_op in cb_ops]
            # Copy the "input_symmetry" to "min_cell_symmetry" as it isn't technically
            # the input symmetry to dials.symmetry
            d["min_cell_symmetry"] = d["input_symmetry"]
            del d["input_symmetry"]
            json_str = json.dumps(d, indent=2)
            with open(params.output.json, "w") as f:
                f.write(json_str)

        # Change of basis operator from input unit cell to best unit cell
        cb_op_inp_best = result.best_solution.subgroup["cb_op_inp_best"]
        # Get the best space group.
        best_subsym = result.best_solution.subgroup["best_subsym"]
        best_space_group = best_subsym.space_group().build_derived_acentric_group()
        logger.info(
            tabulate(
                [[str(best_subsym.space_group_info()), str(best_space_group.info())]],
                ["Patterson group", "Corresponding MX group"],
            )
        )
        # Reindex the input data
        experiments, reflection_tables = _reindex_experiments_reflections(
            experiments, reflection_tables, best_space_group, cb_op_inp_best
        )

    elif params.laue_group is not None:
        if params.change_of_basis_op is not None:
            cb_op = sgtbx.change_of_basis_op(params.change_of_basis_op)
        else:
            cb_op = sgtbx.change_of_basis_op()
        # Reindex the input data
        experiments, reflection_tables = _reindex_experiments_reflections(
            experiments, reflection_tables, params.laue_group.group(), cb_op
        )

    if params.systematic_absences.check:
        logger.info("=" * 80)
        logger.info("")
        logger.info("Analysing systematic absences")
        logger.info("")

        # Get the laue class from the current space group.
        space_group = experiments[0].crystal.get_space_group()
        laue_group = str(space_group.build_derived_patterson_group().info())
        logger.info("Laue group: %s", laue_group)
        if laue_group in ("I m -3", "I m m m"):
            if laue_group == "I m -3":
                logger.info(
                    """Space groups I 2 3 & I 21 3 cannot be distinguished with systematic absence
analysis, due to lattice centering.
Using space group I 2 3, space group I 21 3 is equally likely.\n"""
                )
            if laue_group == "I m m m":
                logger.info(
                    """Space groups I 2 2 2 & I 21 21 21 cannot be distinguished with systematic absence
analysis, due to lattice centering.
Using space group I 2 2 2, space group I 21 21 21 is equally likely.\n"""
                )
        elif laue_group not in laue_groups_for_absence_analysis:
            logger.info("No absences to check for this laue group\n")
        else:
            if not refls_for_sym:
                refls_for_sym = get_subset_for_symmetry(
                    experiments, reflection_tables, params.exclude_images
                )

            if (params.d_min is Auto) and (result is not None):
                d_min = result.intensities.resolution_range()[1]
            elif params.d_min is Auto:
                d_min = resolution_filter_from_reflections_experiments(
                    refls_for_sym,
                    experiments,
                    params.min_i_mean_over_sigma_mean,
                    params.min_cc_half,
                )
            else:
                d_min = params.d_min

            # combine before sys abs test - only triggers if laue_group=None and
            # multiple input files.
            if len(reflection_tables) > 1:
                joint_reflections = flex.reflection_table()
                for table in refls_for_sym:
                    joint_reflections.extend(table)
            else:
                joint_reflections = refls_for_sym[0]

            merged_reflections = prepare_merged_reflection_table(
                experiments, joint_reflections, d_min
            )
            run_systematic_absences_checks(
                experiments,
                merged_reflections,
                float(params.systematic_absences.significance_level),
            )

    logger.info(
        "Saving reindexed experiments to %s in space group %s",
        params.output.experiments,
        str(experiments[0].crystal.get_space_group().info()),
    )
    experiments.as_file(params.output.experiments)
    if params.output.reflections is not None:
        if len(reflection_tables) > 1:
            joint_reflections = flex.reflection_table()
            for table in reflection_tables:
                joint_reflections.extend(table)
        else:
            joint_reflections = reflection_tables[0]
        logger.info(
            "Saving %s reindexed reflections to %s",
            len(joint_reflections),
            params.output.reflections,
        )
        joint_reflections.as_file(params.output.reflections)

    if params.output.html and params.systematic_absences.check:
        ScrewAxisObserver().generate_html_report(params.output.html)

except ValueError as e:
    sys.exit(e)
end_symmetry_time = time()
logger.info("Symmetry cost %s s"%(end_symmetry_time - start_symmetry_time))
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


###############################################################################################################
###############################################################################################################
###############################################################################################################
############################                                                       ############################
############################                                                       ############################
############################                        DIALS  SCALE                   ############################
############################                                                       ############################
############################                                                       ############################
###############################################################################################################
###############################################################################################################
###############################################################################################################

#########################################       SCALE DEFINITION       ########################################

from dials.command_line.scale import phil_scope, _export_unmerged_mtz, _export_merged_mtz
# from dials.util import log
# from dials.util.options import ArgumentParser, reflections_and_experiments_from_files
from dials.algorithms.scaling.algorithm import ScaleAndFilterAlgorithm, ScalingAlgorithm


###############################################################################################################
start_scale_time = time()
scale_phil = copy.deepcopy(phil_scope)
input_phil_scope = generate_input_scope(read_experiments=True, read_reflections=True)
if input_phil_scope is not None:
    scale_phil.adopt_scope(input_phil_scope)

params = scale_phil.extract()


reflections, experiments = reflections_and_experiments_from_files(
    joint_reflections, experiments
)

try:
    # scaled_experiments, joint_table = run_scaling(params, experiments, reflections)

    """Run scaling algorithms; cross validation, scaling + filtering or standard.

    Returns:
        experiments: an experiment list with scaled data (if created)
        joint_table: a single reflection table containing scaled data (if created).
    """
    params.exclude_images = []
    if params.output.delete_integration_shoeboxes:
        for r in reflections:
            del r["shoebox"]
    
    if params.cross_validation.cross_validation_mode:
        from dials.algorithms.scaling.cross_validation.cross_validate import (
            cross_validate,
        )
        from dials.algorithms.scaling.cross_validation.crossvalidator import (
            DialsScaleCrossValidator,
        )

        cross_validator = DialsScaleCrossValidator(experiments, reflections)
        cross_validate(params, cross_validator)

        logger.info(
            "Cross validation analysis does not produce scaling output files, rather\n"
            "it gives insight into the dataset. Choose an appropriate parameterisation\n"
            "and rerun scaling without cross_validation_mode.\n"
        )
        scaled_experiments, joint_table =  (None, None)
    
    else:
        if params.filtering.method:
            algorithm = ScaleAndFilterAlgorithm(params, experiments, reflections)
        else:
            algorithm = ScalingAlgorithm(params, experiments, reflections)

        algorithm.run()

        scaled_experiments, joint_table = algorithm.finish()

except ValueError as e:
    raise Sorry(e)
else:
    # Note, cross validation mode does not produce scaled datafiles
    if scaled_experiments and joint_table:
        logger.info(
            "Saving the scaled experiments to %s", params.output.experiments
        )
        scaled_experiments.as_file(params.output.experiments)
        logger.info(
            "Saving the scaled reflections to %s", params.output.reflections
        )
        joint_table.as_file(params.output.reflections)
        if params.output.unmerged_mtz:
            _export_unmerged_mtz(params, scaled_experiments, joint_table)
        if params.output.merged_mtz:
            _export_merged_mtz(params, scaled_experiments, joint_table)
logger.info(
    "See dials.github.io/dials_scale_user_guide.html for more info on scaling options"
)
end_time = time()
logger.info('Scale cost %s s'%(end_time - start_scale_time))
logger.info('The whole pipeline cost %s s'%(end_time - start_time))