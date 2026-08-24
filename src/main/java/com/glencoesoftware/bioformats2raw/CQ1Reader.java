/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import loci.common.Location;
import loci.common.RandomAccessInputStream;
import loci.formats.FormatException;
import loci.formats.MetadataTools;
import loci.formats.in.OMETiffReader;
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;

import ome.units.quantity.Length;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Image;
import ome.xml.model.Plate;
import ome.xml.model.Well;
import ome.xml.model.WellSample;
import ome.xml.model.primitives.NonNegativeInteger;

/**
 * Reader for Yokogawa CQ1 data, which is largely based on OME-TIFF.
 */
public class CQ1Reader extends OMETiffReader {

  private List<String> extraFiles = new ArrayList<String>();
  private transient boolean addPlateAcquisition = false;
  private transient boolean addPlateName = false;

  /** Construct a new CQ1 reader. */
  public CQ1Reader() {
    super("Yokogawa CQ1", new String[] {"ome.tif", "ome.tiff"});
  }

  @Override
  public boolean isThisType(String name, boolean open) {
    if (checkSuffix(name, "companion.ome")) {
      return false;
    }
    return super.isThisType(name, open);
  }

  @Override
  public boolean isThisType(RandomAccessInputStream stream) throws IOException {
    boolean valid = super.isThisType(stream);
    if (!valid) {
      return false;
    }
    String creator = getStoredMetadata().getCreator();
    return creator != null && creator.startsWith("Yokogawa, CQ");
  }

  @Override
  public void close(boolean fileOnly) throws IOException {
    super.close(fileOnly);
    if (!fileOnly) {
      extraFiles.clear();
      addPlateAcquisition = false;
      addPlateName = false;
    }
  }

  @Override
  public String[] getSeriesUsedFiles(boolean noPixels) {
    ArrayList<String> allFiles = new ArrayList<String>();
    allFiles.add(currentId);
    allFiles.addAll(extraFiles);

    if (noPixels) {
      return allFiles.toArray(new String[allFiles.size()]);
    }
    for (String f : super.getSeriesUsedFiles(noPixels)) {
      if (!allFiles.contains(f)) {
        allFiles.add(f);
      }
    }
    return allFiles.toArray(new String[allFiles.size()]);
  }

  /* @see loci.formats.SubResolutionFormatReader#initFile(String) */
  @Override
  protected void initFile(String id) throws FormatException, IOException {
    super.initFile(id);

    findExtraFiles(new Location(currentId).getParentFile());

    // set the plate name to something useful

    if (addPlateName) {
      String[] files = getSeriesUsedFiles();
      Location pixels = new Location(files[files.length - 1]).getAbsoluteFile();
      pixels = pixels.getParentFile();
      String pixelsPath = pixels.getName();
      String parentDirectory = pixels.getParentFile().getName();
      metadataStore.setPlateName(parentDirectory + "_" + pixelsPath, 0);
    }
  }

  @Override
  protected void convertMetadata(OMEXMLMetadata meta, MetadataStore store) {
    // remove existing plate acquisitions
    meta.resolveReferences();
    OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) meta.getRoot();
    Plate plate = root.getPlate(0);

    // if only one PlateAcquisition, keep it (expected for newer data)
    // older data may have one PlateAcquisition per WellSample,
    // in which case all PlateAcquisitions should be removed
    int plateAcqCount = plate.sizeOfPlateAcquisitionList();
    if (plateAcqCount > 1) {
      while (plate.sizeOfPlateAcquisitionList() > 0) {
        plate.removePlateAcquisition(plate.getPlateAcquisition(0));
      }
      addPlateAcquisition = true;
    }

    String originalPlateName = plate.getName();
    if (originalPlateName == null || originalPlateName.isEmpty() ||
      originalPlateName.equalsIgnoreCase("microplate"))
    {
      // don't set the plate name here as it's based on the
      // path of the last used file (which hasn't been found yet)
      addPlateName = true;
    }

    // remove the first Image, if it is not linked to the plate

    Image firstImage = root.getImage(0);
    if (firstImage.sizeOfLinkedWellSampleList() == 0) {
      LOGGER.warn("removing first image");
      root.removeImage(firstImage);
    }

    // remove any screens

    while (root.sizeOfScreenList() > 0) {
      root.removeScreen(root.getScreen(0));
    }

    // reset Image links
    // call to MetadataTools.populatePixels(...) in parent class
    // will erase the links if not proactively reset
    List<String> imageIDs = new ArrayList<String>();
    for (int i=0; i<root.sizeOfImageList(); i++) {
      imageIDs.add(root.getImage(i).getID());
    }
    for (int w=0; w<plate.sizeOfWellList(); w++) {
      Well well = plate.getWell(w);
      for (int ws=0; ws<well.sizeOfWellSampleList(); ws++) {
        WellSample sample = well.getWellSample(ws);
        Image linked = sample.getLinkedImage();
        int index = imageIDs.indexOf(linked.getID());
        if (index < 0) {
          LOGGER.warn("Linked image does not exist: {}", linked.getID());
        }
        else {
          linked.setID(MetadataTools.createLSID("Image", index));
          sample.setIndex(new NonNegativeInteger(index));
        }
      }
    }

    meta.setRoot(root);

    try {
      getService().convertMetadata(meta, store);
    }
    catch (FormatException e) {
      LOGGER.error("Could not convert OME-XML metadata", e);
    }

    // add a new plate acquisition that links all well samples

    if (addPlateAcquisition) {
      store.setPlateAcquisitionID(
        MetadataTools.createLSID("PlateAcquisition", 0, 0), 0, 0);
      int nextSample = 0;
      for (int w=0; w<plate.sizeOfWellList(); w++) {
        Well well = plate.getWell(w);
        for (int ws=0; ws<well.sizeOfWellSampleList(); ws++) {
          String ref = well.getWellSample(ws).getID();
          store.setPlateAcquisitionWellSampleRef(ref, 0, 0, nextSample++);
        }
      }
    }

    // set channel names to emission wavelength

    for (int img=0; img<meta.getImageCount(); img++) {
      for (int c=0; c<meta.getChannelCount(img); c++) {
        Length emission = meta.getChannelEmissionWavelength(img, c);
        if (emission != null && emission.value() != null) {
          String value = String.valueOf(emission.value().intValue());
          store.setChannelName(value, img, c);
        }
      }
    }
  }

  /**
   * Scan the given directory for non-OME-TIFF files.
   *
   * @param root directory to scan
   */
  private void findExtraFiles(Location root) {
    String[] list = root.list(true);
    for (String f : list) {
      // OME-TIFFs and files in the "Image"/"Projection" directories are
      // pixels files and will have been detected already
      if (f.equals("Image") || f.equals("Projection") ||
        checkSuffix(f, suffixes))
      {
        continue;
      }
      Location file = new Location(root, f);
      if (file.isDirectory()) {
        findExtraFiles(file);
        continue;
      }
      extraFiles.add(file.getAbsolutePath());
    }
  }

  @Override
  public boolean failOnMissingTIFF() {
    // Allow the CQ1Reader to leniently handle plates
    // with missing TIFF files
    return false;
  }

}
